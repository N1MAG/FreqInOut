# Guided Radio And Software Configuration Specification

Status: authoritative product and implementation specification; policy approved
2026-09-16; GRS-0 through GRS-5 automated exit gates passed 2026-09-17;
operator-assisted live gates remain explicitly pending as listed below

Governing delivery contract: `project_delivery_rules.md`

Governing product/UI contracts:

- `multirig_product_ui_contract.md`
- `task_oriented_workspace_design_guideline.md`
- `ui_layout_standards.md`

Related technical contracts:

- `settings_configuration_assistant_spec.md`
- `multi_instance_software_administration_spec.md`
- `sdr_receiver_control_spec.md`
- `multi_endpoint_scheduler_concurrency_spec.md`
- `sop_schedule_plan_spec.md`
- `production_reliability_and_workflow_remediation_spec.md`

This specification is the authority for `Settings -> Radios -> Add Radio`,
`Receiver Setup`, and the guided handoff into `Settings -> Software`. It
supersedes older guided-setup language that restricts an observer to SDR++ plus
JS8Call, makes RF Guard or Schedule unavailable to observers, or describes
VarAC cluster membership as review-only. Protocol, scheduler, RF-safety, and
software-family contracts remain authoritative except where this specification
explicitly refines their guided configuration behavior.

## Product Outcome

An operator can add a transceiver or receive-only SDR once, choose every
software capability it will use, and finish with a coherent radio-owned
configuration that FIO can explain, launch, monitor, and recover.

The normal experience must be clear enough for a first-time operator while
remaining exact enough for a multi-radio station:

`Radio -> Operating Model -> Software -> Connections -> Safety -> Schedule -> Review & Save`

The workflow is complete only when FIO knows, for every selected application:

- which radio owns or uses it;
- whether the instance is new, imported, shared, manual, or remote;
- the executable and effective launch command;
- the native profile/configuration identity and application-data roots;
- its endpoints, message/log/database paths, and other exclusive resources;
- its startup, dependency, readiness, and health policy;
- which receive, tune, QSY, transmit, PTT, and scheduler capabilities are
  permitted; and
- what FIO wrote, what the operator owns, and what still requires action.

The operator must never have to reconstruct a software instance by combining a
port from one proposal with a profile or storage path from another. Selecting
an instance always selects one atomic identity bundle.

## Product Principles

1. **Radio first, software in the same session.** Add Radio establishes the
   owning radio draft before software instances are proposed. Detailed
   Software Administration remains the reusable implementation authority, not
   a separate setup journey the operator must rediscover.
2. **One guided language.** SDRs and transceivers use the same step positions,
   ownership choices, review language, and recovery model. Content changes by
   capability; steps do not silently vanish or renumber.
3. **Receive-only is enforced, not implied.** An SDR may run reviewed receive
   applications, including JS8Call and Fast Light, but no UI flag or external
   application can promote the FIO radio profile to PTT or transmit authority.
4. **Discovery is evidence, never ownership.** Discovery does not write,
   assign, launch, probe a radio, or silently adopt a candidate.
5. **Managed means precise.** FIO-managed always names what FIO manages:
   durable identity, launch recipe, and, only for a qualified application
   writer, a reviewed native configuration.
6. **Imported means unchanged.** Import retains the candidate's complete
   identity and never silently renumbers ports, relocates storage, or rewrites
   external configuration.
7. **Safety stays visible.** Receiver resource conflicts, transceiver RF Guard,
   schedule mismatches, cluster PTT policy, and disabled capabilities remain
   visible through Review and Health.
8. **The UI remains usable while work runs.** Filesystem discovery, profile
   parsing, endpoint qualification, process inventory, and native configuration
   writes never run on or synchronously wait from the GUI thread.

## Terminology And Durable Identity

### Radio role

- **Transceiver:** a radio capable of transmit and receive. PTT, QSY, scheduler,
  and application transmit behavior remain separately capability-gated.
- **Observer / SDR:** a receive-only radio. It may be tuned automatically only
  through a qualified receive-only adapter. It never receives PTT or transmit
  authority.

### Software instance source

Each radio-scoped software family uses exactly one of these sources:

- **Create a distinct instance:** FIO proposes a new identity, unique resources,
  launch recipe, and supported native configuration actions.
- **Use an existing instance:** the operator explicitly selects one discovered
  or already-configured candidate; its identity bundle is imported unchanged.
- **Connect manually or remotely:** the operator supplies an endpoint, command,
  and paths that FIO cannot discover locally.
- **Use a shared station tool:** permitted only for applications defined as
  station-shared, such as FLMsg or FLAmp by default.
- **Built into FIO:** a capability mapping, not an external launch item, such as
  FIO Spotter.

Every selected family also has a completion policy:

- **Required for this radio:** the radio remains inactive and setup remains
  incomplete until its identity, persistence, and required operator actions
  pass Review.
- **Optional capability:** the radio may be saved, but Review and Health retain
  a per-app `Needs attention` or `Complete later` route.

`Operator starts this application` is a launch policy, not permission to leave a
required instance unidentified. New software selected as part of a preset is
required by default; the operator must deliberately mark it optional or remove
it from the software set.

### Atomic instance bundle

An instance bundle includes all applicable values below and has one stable
draft identity before persistence:

- family, variant, version evidence, and FIO instance key;
- owning radio draft or saved radio ID;
- executable/application path and effective command/arguments;
- working directory and environment/profile selector;
- native profile/config root and application-data root;
- host and named TCP/UDP endpoints;
- rig name, VFO/target, audio route, control owner, and dependencies;
- message, log, form, database, inbox, and outbox paths;
- launch-at-startup, monitor-health, readiness, and execution scope;
- exclusive/shared resource claims and desired/observed fingerprints; and
- provenance, ownership, verification evidence, and recovery state.

The UI and persistence layer may not apply fields from different bundles
independently. For example, selecting an existing JS8Call profile using API
port `2442` restores that candidate's complete bundle. Creating a second
instance may propose `2443`, but the proposal must also carry a distinct rig
name, profile/data root, message files, and launch identity.

## Capability Matrix

| Software family | Observer / SDR | Transceiver | Default authority |
| --- | --- | --- | --- |
| SDR++ receiver | Allowed | Optional only when modeled as a separate receiver | Receive/tune through qualified adapter; never PTT |
| JS8Call, Improved, Subspace | Allowed as a distinct receive/import instance | Allowed | SDR: receive/import only; transceiver: capabilities from operating model and safety policy |
| Fast Light | Allowed in receive-only mode | Allowed | SDR: no PTT, CAT transmit, or FIO send; transceiver: receive-safe by default, TX only through explicit Advanced setup |
| FIO Spotter | Allowed | Allowed | Built-in mapping; observer Compose/Expect/retrieval transmit paths disabled |
| External JS8Spotter | Hidden until a reviewed receive-only contract exists | Allowed when configured | External application policy |
| CommStat | Allowed only in an explicitly receive-only transport mapping | Allowed | Mapping never expands the owning radio's capability |
| VarAC / VarAC Cluster | Not allowed | Allowed | VarAC owns frequency/PTT behavior; FIO does not QSY or schedule VarAC transmissions |
| FLRig transceiver control | Not allowed as observer control | Allowed | Transceiver control only |

Direct persistence and launch-planner validation enforce the matrix. Hiding a
choice in the UI is not the safety boundary.

## Guided Workflow

The seven step positions remain visible throughout the workflow. A step may say
`Not used` with a concise reason, but it does not disappear and cause later
steps to move.

### 1. Radio

The operator chooses or enters:

- hardware manufacturer/model and station-facing name;
- transceiver or receive-only SDR role;
- deployment mode and whether the radio will be available to FIO; and
- optional hardware identity required to prevent duplicate control ownership.

Changing the role after software selection triggers a compatibility review. It
does not silently remove an application. Incompatible choices are named and the
operator chooses whether to discard them or return to the prior role.

### 2. Operating Model

The page lists only compatible enabled models and clearly explains their
capabilities. A fresh station always has protected defaults for a transceiver
and a receive-only SDR.

The operator can:

- select an existing compatible model;
- create a model from a reviewed role-appropriate template; or
- continue with a clearly marked inactive draft when a model must be completed
  later.

The receive-only template enables Maps, Messages, ingest, and approved launch
items while disabling PTT, transmit, Compose sending, Expect replies, retrieval
transmissions, and transmit net-control actions. It may enable receiver
scheduler ownership only after receiver-control verification.

### 3. Software

The page first asks what the radio will use, then expands one concise card for
each selected family. Presets such as JS8, Fast Light, TriMode, VarAC, or SDR
Receive may preselect cards but do not bypass their configuration.

Each external family requires an explicit instance-source choice. The
recommended choice for a second local radio or SDR is **Create a distinct
instance**. Choosing **Use an existing instance** reveals bounded candidates.

The page renders immediately from a cached discovery snapshot. If the snapshot
is absent or stale, FIO may begin one bounded background discovery after the
operator selects a family. `Find installed software` remains available for an
explicit refresh. The page stays fully navigable while discovery runs.

When a selected family needs the mature Software Administration assistant, Add
Radio embeds that same core workflow against the unsaved radio draft. It must
not present a reduced second implementation. If a platform limitation requires
leaving Add Radio, FIO saves a resumable inactive setup draft and opens the exact
`Settings -> Software -> <family> -> <radio draft>` task. Returning resumes at
the same guided step with prior choices intact.

### 4. Connections

Connections are grouped into separately titled cards by responsibility. At
minimum, an observer using SDR++ and JS8Call sees:

- **Receiver control — SDR++:** adapter, stable target/VFO, host, port, test
  evidence, and FIO-tuning opt-in.
- **Receive/decode companion — JS8Call:** executable variant, rig/profile
  identity, API/UDP endpoints, message/data paths, and launch policy.

Fast Light similarly separates FLRig control, FLDigi modem/logs, shared
FLMsg/FLAmp tools, and their relationship. VarAC separates node-local paths and
connections from optional cluster-shared configuration.

The primary receiver action is labeled **Test SDR++ receiver control** or the
exact adapter name. Its summary names the endpoint and target. It never appears
to test JS8Call, FLDigi, or another companion application.

Test Control performs the reversible tune/readback/restore contract on the
background endpoint lane. A passed test applies only to the exact adapter,
host, port, target, and application evidence. Editing any identity field
invalidates the evidence. Manual tuning remains available after failure.

### 5. Safety

The title adapts without changing position:

- **Receiver Guard** for an observer;
- **RF Guard** for a transceiver.

Receiver Guard configures shared antenna, frontend, preselector, converter, and
other receive-resource groups. Verified automatic receiver tunes participate in
central resource arbitration. Receiver Guard never reads or grants PTT and does
not imply transmit protection that the hardware cannot provide.

Receiver setup uses these visible states consistently in Connections, Safety,
Schedule, Review, and Health:

- **Not configured:** no complete receiver adapter identity exists.
- **Manual / reminders only:** the receiver is usable, but no matching control
  evidence authorizes automatic retuning.
- **Verified — automatic retune allowed:** current evidence matches the saved
  adapter, endpoint, target, and application identity.
- **Blocked by receiver resource:** control is verified but a declared shared
  antenna/frontend resource currently holds the tune.
- **Verification expired or changed:** evidence became stale or an identity
  field changed; scheduling falls back to reminders until retested.

RF Guard retains antenna-band validation, shared PTT/antenna/frontend/amplifier
groups, unsupported-band policy, and selected-plan checks. Fast Light advanced
TX, JS8 transmit, and VarAC configuration do not bypass central RF safety.

For VarAC Cluster, the page also summarizes the cluster PTT-lock policy. Until
the runtime actually enforces a specific VarAC cluster interlock, the UI labels
it as an operator/VarAC-owned policy rather than claiming FIO enforcement.

### 6. Schedule

The title and available rows reflect the radio role:

- **Receive Schedule:** an observer with matching receiver-control evidence may
  receive automatic SDR++ retunes. A manual or unverified receiver may use the
  same plan for reminders and acknowledgements without an automatic command.
- **Radio Schedule:** a transceiver uses the normal scheduler, RF Guard, PTT,
  expected-state, and ownership contracts.

VarAC-only activity remains monitor/import/launch oriented. FIO does not QSY or
schedule VarAC transmissions. A transceiver may still have a radio schedule for
its other software lanes; Review identifies which applications are controlled
by FIO and which remain application-owned.

The schedule page uses the same plan-validation result shown in Safety and lists
all mismatches before save.

### 7. Review & Save

Review is a readable operational plan, not a raw field dump. It shows:

- radio role, model, activation/default intent, and control owner;
- every selected family exactly once;
- instance source, ownership, variant, paths, endpoints, and exclusive claims;
- exact effective launch command, working directory, dependencies, startup and
  health flags;
- native configuration actions, backup targets, and operator-owned actions;
- receive/tune/QSY/PTT/transmit/scheduler permissions;
- Receiver Guard or RF Guard assignments and schedule result;
- VarAC node and optional cluster membership, with node-local and shared
  resources separated;
- conflicts, warnings, verification evidence, and recovery consequences; and
- the state that will remain if an optional live application is unavailable.

Review contains a visible **Launch plan** section. It lists every external
component, exact effective command or operator-start state, dependencies,
execution scope, startup choice, readiness policy, and the distinction between
known recipe, startup inclusion, and bundle enabled. This section is not hidden
inside generic Advanced details.

The primary action is **Save Radio and Software** when software is part of the
transaction. A radio-only save is not reported as complete if selected required
software failed to persist. Optional post-save verification is reported
separately from persistence.

## Software-Family Contracts

### SDR++ And Receiver Applications

SDR++ is the first approved receiver application. FIO stores its executable or
platform launch target, effective command, process-readiness policy, control
adapter, endpoint, and stable target. FIO does not claim native SDR++ device or
module configuration unless a separately qualified writer is added.

The receiver launch item carries `execution_scope=receive_only`. Launch and
control are separate: Test Control may qualify an operator-started SDR++, and a
launch recipe may exist without granting FIO tuning.

### JS8Call Family

Stock JS8Call, JS8Call Improved, and Subspace use the same instance lifecycle.
The operator first chooses the executable variant, then creates, imports, or
manually describes one complete bundle.

A new local instance proposal includes:

- stable FIO instance and `--rig-name` identity;
- distinct native profile/config and data/message roots;
- collision-free TCP API and applicable UDP ports;
- radio-scoped `DIRECTED.TXT` and other ingest paths;
- effective launch command and startup/readiness policy; and
- receive-only execution scope when assigned to an observer.

The port allocator checks saved application records, manifests, launch bundles,
the current transaction, and family-internal conflicts. A new proposal may use
`2443` when `2442` belongs to an existing instance, but the entire new identity
must use the new proposal. Importing the `2442` instance imports its whole
bundle unchanged.

On an observer, FIO never issues JS8 transmit, Expect, retrieval, Compose, or
PTT actions. A managed native writer must also apply and verify the reviewed
receive-only settings it claims to own; otherwise Review identifies the exact
operator step and keeps the instance unverified.

### Fast Light

Fast Light receives the same create/import/manual lifecycle as JS8Call.

A transceiver Fast Light family contains radio-scoped FLRig and FLDigi
instances, their explicit control relationship, and station-shared FLMsg and
FLAmp tools by default. Advanced setup may give FLMsg/FLAmp radio-specific paths
where operational attribution requires them.

An observer Fast Light family is receive-safe by construction:

- FLDigi receive/decoder, logs, and approved shared FLMsg/FLAmp file workflows
  may be configured and launched;
- FLRig/CAT/PTT control, FIO transmit/send automation, and automatic RF actions
  are absent;
- selecting or launching FLMsg/FLAmp does not grant transmit authority;
- Advanced setup may expose audio, log, form, and storage details, but cannot
  promote an observer to transmit capability; and
- any future TX-capable Fast Light option requires a transceiver role, explicit
  Advanced acknowledgement, RF Guard, and final send/preflight enforcement.

For an observer, Fast Light means FLDigi receive/decoder plus selected shared or
radio-specific FLMsg/FLAmp file tools. FLRig is not used as an observer control
route. Advanced TX means any configuration that can key PTT, enable CAT
transmit control, invoke a transmit macro or automatic send, place work on a TX
queue, or authorize FIO to request transmission. Those capabilities are
unavailable for an observer regardless of external application settings.

Every observer-owned FLDigi or radio-specific FLMsg/FLAmp launch item carries
`execution_scope=receive_only`; no FLRig observer launch item exists. A
deliberately shared FLMsg/FLAmp process uses
`execution_scope=station_shared_utility`, which grants no radio control, PTT, or
automatic-send authority. FIO evaluates any operator-requested send against the
selected owning radio at final preflight. A Fast Light component that is itself
configured for automatic TX cannot use the shared-utility identity and requires
a distinct transceiver-scoped launch identity.

A distinct Fast Light proposal records unique FLRig/FLDigi endpoints where
applicable, supported native profile roots, separate attributable FLDigi logs,
FLDigi-to-FLRig relationship, executable paths, effective commands, and
version-qualified readiness. FIO never claims a native profile writer for an
unsupported version.

Fast Light persistence distinguishes three identities:

1. station-shared FLMsg/FLAmp executable and default message-root identity;
2. each radio's Fast Light workflow and its use of those shared tools; and
3. an Advanced radio-specific FLMsg/FLAmp instance with exclusive paths and its
   own launch identity.

Shared roots are recorded deliberately as non-exclusive. Radio-specific roots,
FLRig/FLDigi endpoints, native profile roots, attributable logs, and check-in
paths are collision-checked resource claims. FLRig precedes its linked FLDigi;
FLMsg or FLAmp depends on FLDigi only when the selected integration requires it.
No dependency is invented for an independent receive-only file workflow.

### VarAC And VarAC Cluster

VarAC is available only to transceiver radios. A normal VarAC node has a
distinct launcher/effective command, INI, database/runtime paths, inbox/outbox,
working directory, launch identity, manifest, and radio assignment. FIO may
manage the durable identity and launch recipe while VarAC-native settings remain
operator-owned unless a qualified writer is introduced.

The VarAC node record plus software-instance manifest are authoritative for
node-local install/launcher, exact launch command, working directory, INI,
database/runtime, incoming/inbox, outbox, and resource claims. Device-profile
fields are compatibility projections, not a competing owner. If the current
node schema lacks a first-class field such as outbox, implementation uses an
additive migration and preserves the existing projected value.

Cluster mode is an explicit branch after node configuration:

- **Standalone VarAC**
- **Join an existing cluster**
- **Create a cluster and add this node**

Membership records cluster ID, unique positive instance number, shared database
where applicable, counter refresh, gateway handler, and PTT-lock policy. Node
creation precedes membership. Standalone VarAC never implies cluster mode.

The cluster record owns normalized cluster ID, shared database, counter refresh,
gateway selection, and PTT-lock policy. Membership owns the cluster reference,
radio/node reference, enabled state, and a positive instance number unique among
enabled members of that cluster. Public cluster IDs are unique
case-insensitively after normalization. A shared cluster database is an explicit
non-exclusive resource owned by one cluster identity; it is never substituted
for a node-local VarAC database.

Creating the first node completes cluster creation, adds and enables the node
membership, and only then permits that enabled member to be selected as gateway,
all within the same reviewed transaction.

Review separates node-local from cluster-shared resources. Duplicate node
paths, launch identities, exclusive databases, or active cluster instance
numbers fail before mutation. FIO preserves version-qualified or
operator-provided effective commands and does not invent unverified VarAC
arguments. Linux Wine wrappers and working directories remain part of the
atomic launch identity.

VarAC owns its frequency and transmit behavior. FIO can launch, monitor, ingest,
and coordinate declared shared resources, but it does not issue VarAC QSY or
scheduled-transmit commands.

Review states whether native VarAC automatic transmit/PTT behavior is known,
disabled, enabled, or unknown. Launching VarAC is not RF/transmit authorization
and does not claim FIO can interlock a later native VarAC transmission. A manual
or remote node may be monitored and shown as **Operator starts remotely**, but
FIO creates no local executable launch item for it.

### Built-In And Supporting Applications

FIO Spotter is a built-in capability mapping and creates no external launch
item. External Spotter and CommStat use their own reviewed instance contracts.
For an observer, any supporting application must have a receive-only execution
scope and cannot access send/preflight paths. Unsupported combinations are
shown with a concise reason rather than silently omitted after selection.

## Native Configuration Writer Contract

FIO may create or update third-party native configuration only through an
application-, variant-, version-, and platform-qualified writer. There is no
generic best-effort writer.

A supported writer must:

1. detect the exact installed variant/version and target profile;
2. build a bounded change plan without writing;
3. preview every target file and material setting;
4. validate ports, paths, identities, permissions, and role capabilities;
5. create a recoverable backup of every existing target;
6. write through a temporary file or application-supported atomic mechanism;
7. read back and semantically verify the result;
8. restore backups if apply/readback or the following FIO transaction fails;
9. persist desired and observed fingerprints without credentials; and
10. provide an operator-visible recovery path.

Where no qualified writer exists, FIO still creates the isolated FIO identity,
paths, and launch recipe. It then launches or opens the exact application
profile, presents a short version-appropriate checklist, preserves a resumable
inactive setup draft, and resumes verification in the same guided session. It
labels that state **Configured in FIO — native profile unverified** or
**Operator action required**; it must not label the native profile verified
prematurely.

## Discovery And Performance Contract

### One discovery coordinator

All Add Radio software discovery uses one session-scoped coordinator and one
immutable result snapshot. Application detectors may share phase results; they
may not independently repeat the same JS8 profile scan, process inventory, or
filesystem traversal.

Discovery is:

- limited to configured paths, platform application locations, known profile
  roots, and already-loaded FIO records;
- asynchronous, cancellable at bounded phase boundaries, coalesced, and
  generation-checked;
- free of radio I/O, endpoint probing, process launch, and external writes;
- safe to ignore when the operator changes radio role, family, variant, or
  closes the draft; and
- explicit about candidates, ambiguity, elapsed time, and errors.

The prior snapshot remains usable during refresh. Results fill only a selected
candidate bundle or blank proposal; they never overwrite an operator-edited
bundle.

### GUI responsiveness

- Navigation, typing, selection, resize, paint, and Review rendering perform no
  filesystem, process, database migration, endpoint, radio, or native-profile
  I/O.
- A normal GUI handler returns within 100 ms at the 95th percentile and must not
  synchronously wait longer than the project-wide 250 ms ceiling.
- Discovery status appears within 250 ms of request. Cancel is acknowledged
  within 250 ms and prevents any later UI or persistence mutation; a currently
  uninterruptible system call may finish in its worker without retaining the
  UI.
- Continue and Back remain responsive while discovery runs. A required unresolved
  identity blocks only the affected family's completion, not the whole dialog.
- Endpoint tests, process inventory, native writes, schedule projection, and
  verification use their owning background lanes and publish immutable results.
- One slow application or endpoint cannot delay another family or radio.

### Required telemetry

Structured diagnostics record bounded, non-secret events for:

- discovery request/session/generation IDs;
- phase start, finish, duration, cancellation, and candidate counts;
- reused cache versus actual scan;
- selected candidate/proposal identity without callsigns or private paths in
  ordinary logs;
- port/resource-planning duration and conflict counts;
- native writer preview/backup/apply/readback/restore phases;
- save transaction duration and rollback stage; and
- qualification target family, endpoint class, and outcome.

No raw credentials or unbounded native configuration content enters logs.

## Launch Orchestration Contract

Every configured external application produces either a reviewed launch item or
an explicit **Operator starts this application** state. FIO always knows which
state applies.

The launch item includes:

- radio and software instance identity;
- executable, arguments, working directory, and safe environment/profile
  selector;
- dependency identities and order;
- startup opt-in, monitor-health opt-in, readiness policy, and execution scope;
- endpoints and profile/config/data identity shown in preview; and
- resource conflicts or unresolved operator actions.

Each Fast Light component has its own launch identity. A single family-level
custom command is insufficient: FLRig, FLDigi, FLMsg, and FLAmp each retain an
executable or explicit operator-start state, optional exact command override,
working-directory rule, readiness, dependencies, and startup choice. A shared
FLMsg/FLAmp launch identity may serve multiple radio workflows only when the
operator deliberately selected the station-shared tool.

Known launch recipe, included in startup bundle, and radio startup bundle
enabled are separate states. Saving a known recipe with startup disabled never
causes application-start launch.

For a newly created local managed instance, **Launch with FIO** is recommended
and selected by default, but it has no runtime effect until the radio is saved,
enabled, and included in the station launch plan. Imported, manual, remote, and
shared station applications default to operator-start unless the operator
explicitly opts into FIO launch. Review always shows the effective choice.

Manual start and FIO startup call the same `StationLaunchPlanner`. Shared tools
are deduplicated only by an intentionally shared durable identity. Endpoint-
scoped applications are never deduplicated by process name alone. Dependency
examples include FLRig before its linked FLDigi and SDR++ readiness before a
dependent receive workflow when the operator has requested that ordering.

Launch failure does not corrupt saved configuration. It produces an exact
per-instance `Needs attention` state and recovery action. Launch success proves
process readiness only; semantic endpoint verification remains separate.

## Persistence, Atomicity, And Recovery

The guided session owns a versioned setup draft keyed independently from a
runtime radio. Cancel removes an unsaved draft and makes no runtime or external
change. A deliberately retained draft is inactive, cannot launch, cannot ingest,
and cannot become command focus.

Before mutation, FIO validates the entire transaction:

- radio/model compatibility;
- one-instance-per-family radio assignment;
- instance ownership and expected-current identity for replacement;
- ports, serial/PTT groups, endpoints, paths, message roots, databases, cluster
  numbers, and launch identities;
- safety and schedule compatibility; and
- supported native-writer plans and backups.

Commit order is:

1. validate and freeze the review plan;
2. stage and back up qualified external writes;
3. apply and read back qualified external writes;
4. atomically save the radio, operating-model assignment, application records,
   manifests, radio links, launch bundle, safety groups, schedule, and optional
   cluster membership;
5. restore external backups if the database transaction fails;
6. publish one settings-change event scoped to the affected identities; and
7. run optional asynchronous health/launch verification.

A stale draft, collision, write failure, or database failure leaves the prior
configuration operational and produces no orphan active instance. Replacement
retains the previous record disabled for recovery and changes no unrelated
radio. Disassociation removes only FIO assignments, launch items, and applicable
cluster membership; it never deletes external profiles, logs, messages,
databases, inboxes, or outboxes.

If an unavoidable post-radio compatibility failure occurs on an older database,
the radio remains inactive and Review gives one exact resumable route. The UI
must not report the overall operation as successful.

## UI, Responsive, And Accessibility Contract

The guided workflow uses one vertical scroll owner. At 1920x1080, 1000x700, and
900x560 in Normal and Large Text, light and dark themes:

- steps wrap without truncation and retain their stable order;
- primary content has no page-level horizontal scrollbar;
- the current step, Back, Continue, Cancel, and final Save remain reachable;
- app cards stack or wrap while preserving reading order;
- technical detail uses progressive disclosure but never hides safety state;
- status is communicated by text and icon as well as color;
- every control has an accessible name, keyboard focus, and visible disabled
  reason; and
- application, radio, profile, endpoint, and action labels never use clipped
  ellipses when the distinction affects ownership or safety.

Discovery and endpoint status are inline. A modal is reserved for destructive
replacement/role-change confirmation or a failure that cannot be represented in
the owning card. There is no global tooltip over a section; help belongs to the
specific label or action.

## Error And Recovery Language

Errors state the affected object, what remains safe, and the next action.
Examples:

- `JS8Call profile “Main” already belongs to FTDX-10 on API 2442. Choose that
  existing instance or create a distinct profile.`
- `The SDR++ endpoint 127.0.0.1:4532 did not complete tune/readback/restore.
  Manual tuning remains available.`
- `Fast Light is configured receive-only for RTL-SDR. Transmit controls are not
  available to an observer radio.`
- `VarAC cluster instance 2 is already assigned. Choose another instance number
  before saving.`
- `The radio was retained as an inactive setup draft. Open Settings -> Software
  -> JS8Call -> <radio> to complete native profile verification.`

Generic `failed`, `invalid`, or implementation-internal method names are not
operator-facing guidance.

## Acceptance Matrix

### Core identity and persistence

1. Importing an existing JS8Call `2442` profile imports its complete bundle;
   creating a second instance proposes `2443` plus distinct identity and paths.
2. No UI sequence can produce a new port paired with an existing profile/data
   root unless the operator explicitly edits and passes collision validation.
3. Cancel at every step writes nothing. Restarting a retained incomplete setup
   restores an inactive draft without launch or ingest.
4. Stale/colliding replacement rolls back the radio link, manifest, launch item,
   schedule, and cluster membership together.
5. Qualified external-writer failure restores backups and commits no FIO
   ownership claim.
6. Required-family failure retains an inactive resumable draft; optional-family
   failure saves the radio with an exact per-app `Complete later` route and no
   misleading overall success.

### Observer / SDR

1. Configure RTL-SDR + SDR++ + a new JS8Call or Subspace receive instance in one
   session; verify distinct profile, port, message files, launch command,
   receive-only scope, Receiver Guard, and Receive Schedule.
2. Configure RTL-SDR + SDR++ + Fast Light; FLDigi receive/log workflows and
   approved FLMsg/FLAmp tools launch, while FLRig/CAT/PTT/send controls remain
   unavailable at UI, store, planner, and final-preflight boundaries.
3. Test Control names and tests SDR++ only; JS8 fields cannot change its target.
4. A verified receiver schedule retunes through SDR++; an unverified receiver
   produces reminders only. Neither path reads or waits for PTT.
5. Shared frontend/antenna conflicts hold an automatic observer tune without
   affecting unrelated radios.
6. VarAC and VarAC Cluster cannot be selected or persisted for an observer.
7. After a successful SDR++ test, changing adapter, host, port, target, or
   application identity immediately invalidates evidence and changes the
   schedule to reminders only until retested.
8. Injected or imported Fast Light CAT/PTT/TX/auto-send settings cannot grant an
   observer transmit behavior at UI, store, planner, runtime, or final-preflight
   boundaries.
9. SDR + FIO Spotter, SDR + receive-only CommStat, and SDR + SDR++ + Fast Light
   + JS8Call preserve independent identities while exposing no transmit
   selector.

### Transceiver

1. JS8-only, Fast Light-only, TriMode, VarAC standalone, and custom mixed sets
   configure every selected family through the same guided instance workflow.
2. Fast Light defaults receive-safe; Advanced TX requires explicit capability,
   operating-model, RF Guard, and preflight acknowledgement.
3. FLRig/FLDigi endpoints, profiles, logs, relationship, commands, dependencies,
   and shared versus radio-specific FLMsg/FLAmp paths survive restart.
4. Creating a first VarAC cluster node creates the cluster then membership;
   adding a second radio enforces distinct node identity and cluster instance.
5. VarAC startup uses exact per-node commands and working directories; distinct
   nodes are not deduplicated by process name.
6. VarAC remains outside FIO QSY/scheduled-transmit authority while other lanes
   on the same transceiver retain their valid schedule.
7. Fast Light import preserves component commands, endpoints, paths, and
   sharing without external writes or silent renumbering. Manual per-component
   commands remain exact and receive no generated arguments. Unknown native
   profiles remain `Configured in FIO — native profile unverified`.
8. A transceiver's Advanced Fast Light TX acknowledgement is durable and
   review-visible, while role, endpoint, profile, or RF Guard changes require
   final-preflight revalidation. A stale/injected observer acknowledgement is
   rejected.
9. Case-insensitive duplicate VarAC cluster IDs and duplicate enabled member
   numbers fail before mutation; cluster-shared and node-local databases cannot
   be silently exchanged.

### Launch and platform behavior

1. Every selected external app appears exactly once in Review with the exact
   effective launch command or explicit operator-start state.
2. Manual and startup launch plans are identical except for requested scope.
3. Shared FLMsg/FLAmp launch once when intentionally shared; separate JS8,
   FLRig, FLDigi, SDR++, or VarAC identities launch separately.
4. Linux native/Wine, macOS app/binary, and Windows executable commands retain
   reviewed quoting, profile selectors, and working directories.
5. Unsupported variant/version writers fall back to resumable guided operator
   configuration and never perform a best-effort rewrite.
6. One station-shared FLMsg/FLAmp utility can serve an SDR and transceiver
   workflow with no inherited radio authority. Any automatic-TX use requires a
   separate transceiver-scoped identity.
7. Review exposes the Launch plan to keyboard and accessibility inspection
   without opening generic Advanced details.

### Performance, concurrency, and UI

1. The original slow-discovery scenario with both `js8call` and
   `js8call-subspace` installed performs one bounded profile scan, keeps Back,
   Continue, and Cancel responsive, and reports phase timing.
2. Repeated discovery requests coalesce; stale results cannot alter a changed
   family, role, candidate, or closed draft.
3. Main-thread instrumentation detects no filesystem walk, process snapshot,
   schedule projection, endpoint call, radio I/O, or native-file write in
   navigation, typing, resize, paint, or Review.
4. Slow/unavailable software and receiver endpoints do not block another family
   or radio.
5. The complete workflow passes keyboard, screen-reader naming, light/dark,
   Normal/Large Text, and 1920x1080, 1000x700, and 900x560 geometry checks.
6. A launch/configuration set with the supported maximum radio count remains
   responsive and produces bounded immutable snapshots and logs.
7. FLRig, FLDigi, FLMsg, and FLAmp discovery shares the session coordinator,
   avoids repeated path traversal, and retains per-component candidate
   provenance.
8. Recovery presentation distinguishes `Saved — operator start required`,
   `Saved — verification pending`, `Saved — one app needs attention`, and
   `Saved — launch bundle retry required`, with an exact per-app retry route.

## Delivery Slices And Exit Gates

Implementation must proceed sequentially. A later slice does not begin until
the current gate passes.

### GRS-0 — Authority And Pure State Model

Status: passed 2026-09-17. The pure authority module is
`freqinout/core/guided_radio_software_model.py`; it changes no runtime schema,
persistence path, discovery behavior, launch plan, or UI.

- Reconcile the older SDR, guided setup, software administration, scheduler,
  RF-safety, and launch contracts with this specification.
- Add pure radio-role, family-capability, instance-bundle, and guided-state
  models with no UI or migration.
- Define the application/version writer capability registry.

Exit: contract tests cover every capability-matrix cell and atomic bundle;
existing runtime data is unchanged.

### GRS-1 — Shared Discovery And Proposal Planning

Status: passed 2026-09-17. The Qt-free coordinator, scanner adapters, and pure
proposal planner are `guided_software_discovery.py`,
`guided_software_discovery_sources.py`, and `guided_software_proposals.py`.
Settings Add Radio and explicit Software Auto-Fill share the coordinator and
perform discovery only from worker threads. This slice changes no runtime
schema, native application configuration, endpoint, radio, or ownership.

- Replace redundant Add Radio scans with one coordinator and immutable cache.
- Produce complete JS8, Fast Light, receiver, and VarAC candidates/proposals.
- Add structured timing/cancellation telemetry and deterministic port/resource
  planning.

Exit: bounded/cancel/stale-result/performance tests pass; no discovery writes or
GUI-thread I/O occur; the `2443`/existing-profile regression is impossible.

### GRS-2 — Unified Guided UX

Status: passed 2026-09-17. Add Radio now uses the stable seven-position
workflow for both transceivers and observers, adapts Guard and Schedule content
without hiding steps, and opens the authoritative Software Instance Assistant
against an opaque unsaved-radio owner key rather than a fake persisted ID.
Cancel remains a no-write boundary; app preparation is preview-only until the
final reviewed transaction in a later slice.

- Bind Add Radio to the shared Software Administration workflow and draft.
- Implement stable seven-step content, responsibility cards, resumable handoff,
  Receiver Guard, Receive Schedule, and complete Review.
- Preserve ordinary transceiver behavior while adding all-family guided setup.

Exit: SDR and transceiver screenshot-shaped workflows pass all responsive,
theme, text-size, accessibility, cancel, and draft-resume gates.

### GRS-3 — Family Completion

Status: passed 2026-09-17. JS8Call, receive-safe Fast Light, explicit
transceiver Advanced TX acknowledgement, and VarAC standalone/create/join now
share the reviewed Software Instance Assistant and atomic store boundary.
Observer Fast Light produces FLDigi-only receive-scope launch evidence; direct
or imported FLRig/TX authority and all observer VarAC paths fail closed. VarAC
cluster creation, membership, gateway selection, exact launch identity, and
normalized shared/node-local path checks commit or roll back together.

- Complete JS8 new/import/manual instance behavior.
- Add reviewed receive-only Fast Light scope and transceiver Advanced TX gate.
- Complete VarAC standalone/create-cluster/join-cluster guided paths.
- Enforce observer rejection for VarAC at store and planner boundaries.

Exit: family acceptance matrix, collision, replacement, cluster, and direct
persistence safety tests pass.

### GRS-4 — Native Writers And Launch Integration

Status: passed 2026-09-17. JS8Call native configuration is write-enabled
only for an exact registered family/version/platform/operation and explicit
`.ini` target. Final Save runs previewed backup/apply/readback/restore work on
a Settings-owned worker, restores exact native targets after writer or later
persistence failure, and records either verified native apply evidence or an
operator-action recovery state. Unsupported and incomplete identities never
write native configuration. Launch review now preserves exact command,
arguments, working directory, environment/profile selector, dependency,
readiness, execution-scope, monitor, startup-inclusion, bundle-enabled, and
operator-start state. Manual and startup execution share the same station
planner and differ only by requested radio scope. Fast Light retains each
selected component; FLMsg/FLAmp without a radio-specific recipe remain visible
as explicit operator-start components rather than disappearing.

- Implement only evidence-qualified native writers, backups, readback, restore,
  and resumable fallback.
- Generate complete launch items/dependencies and unify startup/manual preview.
- Resolve startup opt-in versus known recipe versus bundle-enabled semantics.

Exit: writer fault-injection and rollback tests pass; every configured app has
an exact launch or operator-start state; platform command tests pass.

### GRS-5 — Scheduler, Guard, Integration, And Live Qualification

Status: automated implementation gate passed 2026-09-17. Receiver Guard now
persists observer antenna and front-end claims into the central pair-scoped
coordination graph. A conflict involving a verified automatic receiver is a
hard hold rather than an unattended prompt; unrelated radios remain outside
that policy. Receive Schedule selection is limited to compatible receive-only
plans for observers, survives final Save, and uses the existing scheduler
coordinator. Manual or evidence-mismatched receivers remain reminder-only.
Cache-only operational summaries carry the exact hold/failure reason and
recovery action through Station Overview. No PTT or transmit authority was
added.

Automated tests do not constitute live external evidence. The following
operator-assisted gates remain pending and block any broader capability claim:

- RTL-SDR with SDR++ tune/readback/restore and shared antenna/front-end hold;
- current macOS and Linux guided Add Radio runs, plus a Windows run;
- a physical transceiver and configured radio-control backend;
- live Fast Light applications;
- stock JS8Call, Improved, and Subspace native/profile behavior; and
- a multi-node VarAC Cluster.

- Persist and execute Receiver Guard and Receive Schedule through the existing
  scheduler coordinator.
- Complete cross-radio resource arbitration and end-to-end recovery telemetry.
- Run full focused/adjacent regression suites and operator-assisted platform,
  radio, RTL-SDR/SDR++, Fast Light, JS8 variants, and VarAC cluster checks.

Exit: no unresolved P0/P1 regressions; performance budgets pass; live external
gates are recorded explicitly rather than inferred from automated tests; release
documentation matches actual supported capabilities.

## Approved Product Decisions

- The JS8 create/import/manual instance logic also governs Fast Light.
- Fast Light is permitted for an SDR only under an enforced receive-only scope;
  Advanced settings cannot grant an observer transmit authority.
- A transceiver may configure every supported software family in Add Radio,
  including standalone and Cluster VarAC.
- FIO records and previews the effective launch behavior for every configured
  application assigned to a radio.
- Qualified native writers use preview, backup, apply, readback, and recovery;
  unsupported writers use a resumable guided handoff.
- Observers use Receiver Guard and Receive Schedule rather than losing the
  safety and schedule steps.
- Verified SDR++ control may perform automatic receive retuning; manual or
  unverified receivers retain reminder-only schedules.
- None of these decisions grants an observer PTT or transmit authority.
