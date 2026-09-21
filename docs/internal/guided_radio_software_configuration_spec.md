# Guided Radio And Software Configuration Specification

Status: authoritative product and implementation specification; policy approved
2026-09-16. GRS-0 through GRS-5, GRS-6.1 through GRS-6.5, and GRS-7.1 through
GRS-7.5 passed their automated implementation gates on 2026-09-17. Linux
operator testing reopened the GRS-8 implementation gate. GRS-9 automated gates
passed, but operator testing exposed an incomplete projection and transaction
contract. GRS-10 is therefore the corrected controlling specification for a
single cross-service prepared bundle, platform-correct launch identity, and a
non-mutating draft boundary. Its automated implementation gate passed on
2026-09-18. GRS-13 is the current controlling specification for
application-native storage, existing-station adaptation, unified Add Radio and
Software Administration persistence, safe station message/publication
services, NAS policy, and stable multi-instance launch. Its implementation gate is **open** pending
maintainer review and implementation. Operator-assisted live release
qualification remains **open and blocked**. No existing application
configuration may be replaced, relinked, cloned, cleaned up, or reused through
this flow until the exact operation is explicitly chosen and its gate passes.

Governing delivery contract: `project_delivery_rules.md`

Governing product/UI contracts:

- `multirig_product_ui_contract.md`
- `task_oriented_workspace_design_guideline.md`
- `ui_layout_standards.md`

Related technical contracts:

- `settings_configuration_assistant_spec.md`
- `multi_instance_software_administration_spec.md`
- `varac_native_cluster_configuration_spec.md`
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

`Radio -> FIO Behavior -> Software -> Connections -> Safety -> Schedule -> Review & Save`

Within **Software**, the normal task sequence is:

`Choose software and source -> choose VarAC arrangement when applicable -> Prepare selected software automatically -> review the prepared plan -> resolve only items needing attention`

Discovery, preparation, and the concise proposed plan always precede any prompt
to enter or correct paths, ports, commands, profiles, databases, or folders.

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
   owning radio draft before software instances are proposed. It reuses the
   Software Administration proposal and assistant core only after it has
   produced a prepared plan; the operator is not sent to administration as the
   first or normal setup step.
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
9. **Intent controls inference.** `Create a distinct instance`, `Use an existing
   instance`, and `Use a shared station service` are materially different
   operations. Discovery may supply an installed executable to all three, but
   it may never turn a distinct-instance request into reuse of an existing
   profile, data root, endpoint, manifest, or launch identity.
10. **Existing configuration is protected evidence.** A new-radio transaction
    reads existing identities only to avoid collisions or to offer an explicit
    reuse choice. It does not edit, relink, clone from, or write an existing
    native profile unless the operator chooses that exact operation at Review.
11. **Preparation precedes administration.** The normal path first discovers,
    allocates, and explains a complete proposal. Detailed Software
    Administration is a correction, import, manual, or advanced-review surface;
    it is never the first mental step and never begins as a blank technical
    form when FIO can derive the answer.
12. **A proposed target is not a native-write claim.** FIO may reserve and show
    managed roots, endpoints, commands, and intended third-party files before
    Save. It says it will create or verify native application configuration only
    when an exact platform/version-qualified writer satisfies the Native
    Configuration Writer Contract.
13. **FIO is the configuration steward.** For every supported and qualified
    application, FIO generates the optimal complete configuration it can prove
    safe: stable identity, collision-free endpoints, managed directories,
    application files, launch command, working directory, dependencies,
    readiness, and radio/service bindings. The operator chooses operational
    intent and reviews the result; the operator is not made to understand or
    reconstruct application-specific conventions that FIO already knows.
14. **Preparation completes the in-memory bundle.** Preparation remains
    non-mutating with respect to the filesystem, third-party applications, and
    durable FIO data, but it must update the generation-fenced radio draft with
    every FIO-owned derived value. “No external writes before Save” never means
    “leave the draft incomplete.” Only operator-owned decisions survive a
    reprepare unchanged; stale generated values are replaced atomically by the
    current prepared generation.
15. **Administration is exception handling.** A qualified prepared bundle does
    not require a trip to Software Administration or a generic Files form.
    Administration opens only for a genuine ambiguity, unsupported recipe,
    explicit import/manual mode, or an operator-requested Advanced review.

## Terminology And Durable Identity

### Radio role

- **Transceiver:** a radio capable of transmit and receive. PTT, QSY, scheduler,
  and application transmit behavior remain separately capability-gated.
- **Receive-only SDR:** a receive-only radio. It may be tuned automatically only
  through a qualified receive-only adapter. It never receives PTT or transmit
  authority.

`Observer / SDR` remains an internal compatibility term only. Operator-facing
Add Radio, Review, Health, and assignment surfaces use exactly **Transceiver**
and **Receive-only SDR**. They do not append a second `(receive-only)` suffix to
a label that already says `Receive-only SDR`.

### Software instance source

Each radio-scoped software family uses exactly one of these sources:

- **Create a distinct instance:** FIO proposes a new identity, unique resources,
  launch recipe, and supported native configuration actions.
- **Use an existing instance:** the operator explicitly selects one discovered
  or already-configured candidate; its identity bundle is imported unchanged.
- **Connect manually or remotely:** the operator supplies an endpoint, command,
  and paths that FIO cannot discover locally.
- **Use an explicit shared service:** permitted only for a deliberately shared
  station or operating-group component with a durable owner, bindings,
  concurrency policy, and qualified launch/storage contract. A shared
  executable alone does not make FLMsg or FLAmp runtime state shared.
- **Built into FIO:** a capability mapping, not an external launch item, such as
  FIO Spotter.

Durable source enums may retain the existing internal names, but Add Radio uses
these operator-facing labels consistently:

| Durable meaning | Add Radio label |
| --- | --- |
| Create a distinct instance | **Create a new FIO-managed instance** |
| Use an existing instance | **Use an existing instance unchanged** |
| Connect manually or remotely | **Set up manually or connect remotely** |
| Use an explicit shared service | **Use station-shared `<service>`** |
| Built into FIO | **Included with FIO** |

For a normal managed family, the visible identity is `Instance for: <radio
name>`. It is a read-only summary and never appends `JS8Call Family`, `Fast
Light`, or another implementation suffix. The opaque draft and system keys
remain internal. **VarAC arrangement** is separate from source: standalone,
create cluster, and join cluster are topology choices made before preparation,
not file or connection fields.

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

For a managed distinct instance, FIO creates an immutable draft key when the
operator chooses `Create a distinct instance`. The radio name supplies the
human-facing label and a sanitized path/rig-name stem. Opaque FIO draft and
application keys remain in FIO persistence and manifests; they do not appear in
operator-facing configuration names or application-native paths. A qualified
family may use a documented application-required suffix, but it may not append
an internal FIO identifier merely for convenience. Back/Next navigation,
discovery refresh, and later display-name changes do not silently change native
profile roots, ports, selectors, or launch identity. Renaming or relocating a
native identity is a separate reviewed clone/migrate operation. If two radio
names sanitize to the same exclusive native identity, FIO reports the collision
and asks for a distinct meaningful radio name rather than exposing an opaque
hash.

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

## Task-Oriented Redesign Brief

- **Primary operator task:** Add a radio and prepare a safe, distinct, launchable
  software set without having to understand application-specific storage,
  command-line, or database conventions.
- **Starting context:** The operator entered `Settings -> Radios -> Add Radio`,
  named the radio, selected its hardware role, and chose how FIO should use it.
  Existing station configuration is evidence for recommendations and collision
  avoidance; it is not permission to reuse or modify anything.
- **Completion outcome:** One reviewed transaction saves the radio, FIO Behavior,
  prepared application identities, manifests, launch plan, safety and schedule
  assignments, and any explicitly chosen VarAC cluster operation. Cancel or a
  failure leaves all prior configuration unchanged.
- **Normal task sequence:** Choose software and source; choose the VarAC
  arrangement when VarAC is selected; prepare automatically; scan one concise
  readiness card per family; resolve only exceptions; review; save.
- **Primary action:** `Prepare selected software automatically` is the dominant
  action on Step 3 after the required source and VarAC-arrangement choices are
  complete. `Save Radio and Software` is the sole final commit action.
- **Essential state and Why:** The radio, source/mode, proposed ownership,
  readiness, non-conflicting endpoint summary, launch policy, safety impact,
  and reason for any recommendation or block remain visible. Exact paths,
  commands, fingerprints, dependencies, and raw claims are available through
  `Show details` unless the operator must act on them.
- **Secondary and advanced work:** `Find installed software` refreshes the
  bounded cache. Import, manual/remote setup, executable correction, custom
  commands, raw paths, and diagnostics appear only after preparation or through
  an explicit Advanced route.
- **Workspace archetype:** Guided workflow. The shared Software Instance
  Assistant consumes an already-prepared immutable plan and corrects or reviews
  it; it does not replace the normal Add Radio sequence with schema-shaped
  administration.
- **Responsive behavior:** One fixed header and one fixed action footer surround
  one vertical scroll owner. Step controls wrap in stable order. Prepared cards
  stack in the same reading order at medium and compact widths. Expanding
  details cannot move the footer offscreen or introduce page-level horizontal
  scrolling.
- **Shared-theme and component reuse:** Step chips, source selectors, status
  banners, readiness cards, buttons, disclosure controls, focus, warning, and
  disabled states use `freqinout/gui/theme.py` and established shared helpers.
  This workflow introduces no local palette or fixed text-bearing metric.
- **Performance boundary:** Rendering uses one immutable configuration-inventory
  snapshot. Discovery and proposal work is bounded, asynchronous, coalesced,
  cancellable, and generation-fenced. Paint, resize, navigation, typing,
  selection, disclosure toggles, and Review perform no filesystem, process,
  endpoint, radio, migration, or message-history I/O.

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

### 2. FIO Behavior (Operating Model)

Step 1 already establishes the hardware role as **Transceiver** or
**Receive-only SDR**. Step 2 does not ask that question again. It answers the
operator question **How should FIO use this radio?** and may be titled
`FIO Behavior` while retaining `Operating Model` as the durable configuration
term.

The page lists only role-compatible enabled models and explains their behavior
in plain language. A fresh station always has protected defaults named
**Standard transceiver operations** and **Receive-only monitoring**. A model
name must not look like a schedule or frequency plan. In particular,
`Daily HF Schedule` is not an acceptable default Operating Model label; plan
and schedule choices belong only in Step 6.

The operator can:

- select an existing compatible model;
- create a model from a reviewed role-appropriate template; or
- continue with a clearly marked inactive draft when a model must be completed
  later.

The receive-only template enables Maps, Messages, ingest, and approved launch
items while disabling PTT, transmit, Compose sending, Expect replies, retrieval
transmissions, and transmit net-control actions. It may enable receiver
scheduler ownership only after receiver-control verification.

The selected model is summarized by capabilities rather than repeating the
role in a suffix. Advanced reusable models remain available when their behavior
genuinely differs, but the normal path requires no knowledge of FIO's model,
plan, or assignment schema.

### 3. Software

The first visible section asks what the radio will use and presents the primary
preparation action. It does not begin with responsibility cards, raw fields, or
the title `Software Administration`. Presets such as JS8, Fast Light, TriMode,
VarAC, or SDR Receive may preselect families but do not bypass their source or
arrangement decisions.

The normal sequence is binding:

1. **Choose software.** Select the families this radio will use.
2. **Choose source.** For each external radio-scoped family choose `Create a new
   FIO-managed instance` (recommended for a second local radio), `Use an
   existing instance unchanged`, or `Connect manually or remotely`. FIO Spotter
   says `Included with FIO`; CommStat says `Use the station CommStat service`.
3. **Choose VarAC arrangement.** When VarAC is selected, show the current
   topology and choose standalone, create-cluster, join-cluster, import, or
   manual/remote intent before showing node or cluster detail fields. The
   conditional rules are defined in the VarAC family contract below.
4. **Prepare selected software automatically.** This is the dominant action and
   replaces the ambiguous `Configure Automatically` label. It resolves binaries,
   qualified recipes, generated roots, collision-free resources, launch policy,
   shared-service bindings, and applicable VarAC topology into complete atomic
   proposals.
5. **Review the prepared plan.** Show one compact family card with `Ready`,
   `Needs attention`, `Using existing unchanged`, `Manual setup required`,
   `Discovery in progress`, or `Stale — reprepare required`.
6. **Resolve only exceptions.** Open the shared assistant only for an unresolved
   choice, missing or ambiguous executable, unsupported recipe, explicit import,
   manual/remote setup, or operator-requested review/Advanced detail.

The source and VarAC-arrangement selectors are intent decisions, not technical
configuration fields. FIO may use the current cached inventory to explain those
choices immediately. If the cache is absent or stale, the operator's explicit
Prepare action starts one bounded background refresh without blocking the step.
The operator is never prompted to enter or correct paths, ports, commands,
profiles, databases, or folders before automatic preparation has been offered
and its result is available.

Before proposing values, FIO resolves the selected source against one immutable
station-inventory snapshot containing saved application rows, manifests,
launch bundles, device projections, retained drafts in the current transaction,
intentional station-shared services, and conservatively classified incomplete
records. That same snapshot and generation are used by Add Radio, Software
Administration, Review, and final persistence.

The source decisions have these exact boundaries:

| Operator choice | FIO may reuse | FIO must make distinct |
| --- | --- | --- |
| Create a new FIO-managed instance | installed application binary and a qualified version recipe | stable instance key, native profile/config root, application-data/message/log roots, TCP/UDP endpoints, rig/profile selector, manifest, and launch identity |
| Use an existing instance unchanged | the complete selected source-locked bundle | nothing inside that bundle; edits require `Clone as distinct` |
| Use the station service | only an application explicitly defined as shared, plus deliberate radio-to-service bindings | each radio-owned endpoint binding and its capability/safety scope |
| Connect manually or remotely | nothing inferred beyond reviewed evidence | the explicit endpoint/command/path identity entered by the operator |

Using an installed binary is not the same as using an existing instance. A new
managed-instance request never preselects an existing native profile, settings
file, data folder, message file, endpoint, manifest, or incomplete legacy row
merely because it was discovered. If FIO cannot construct a complete
collision-free bundle, the card remains `Needs attention` and names the one
unsupported or unresolved step; it does not manufacture a hybrid bundle.

The prepared family card contains only information needed to decide or act:
application, source/mode, radio identity, allocated endpoint summary, launch
policy, readiness, and a one-line Why. Examples include:

- `JS8Call · New FIO-managed instance · Found · API 2443 · Launch with FIO · Ready`
- `Fast Light · New FIO-managed instance · FLRig + FLDigi found · 12346 / 7363 · Ready`
- `VarAC · Create a cluster with FTDX-10 VarAC · Confirmation required`

Exact paths, commands, environment, selectors, fingerprints, dependencies,
resource claims, and diagnostics are collapsed under `Show details`. A required
warning, collision, unsupported writer, external action, unsafe capability, or
reason the next action is disabled remains visible without expanding details.

The page renders immediately from the prior coherent cached snapshot. `Find
installed software` explicitly refreshes that cache. Preparation reuses the
same in-flight or completed generation rather than starting a second scan. Back,
Next, Cancel, source changes, and family changes remain responsive while work
runs; stale results cannot modify the current draft.

When a selected family needs the shared Software Instance Assistant, it opens
against the unsaved radio draft and receives the prepared immutable proposal.
Its title is task-specific, such as `JS8Call setup for TriMode`, not the generic
`Software Administration`. It is a correction/review surface, not a blank form.
Generated fields are presented as `FIO will create` or `FIO will use` and are
read-only in the normal path. `Browse...` appears only for a missing or ambiguous
executable or after the operator explicitly chooses manual or Advanced setup.

If a platform limitation requires leaving Add Radio, FIO saves a resumable
inactive setup draft and opens the exact
`Settings -> Software -> <family> -> <radio draft>` task with the same prepared
plan. Returning resumes at the same step with intent, proposal, focus, expanded
state, and scroll position intact.

### 4. Connections

Connections consumes the prepared atomic bundles from Step 3; it does not
reconstruct them from loose fields. Normal cards confirm only the operational
target, required endpoint choice, launch-at-startup policy, verification, and
blocked or unsafe state. Generated executable/profile/data roots and exact
recipe details are read-only prepared facts under `Show details`.

Connections are grouped into separately titled cards by responsibility. At
minimum, an observer using SDR++ and JS8Call sees:

- **Receiver control — SDR++:** adapter, stable target/VFO, host, port, test
  evidence, and FIO-tuning opt-in.
- **Receive/decode companion — JS8Call:** executable variant, rig/profile
  identity, API/UDP endpoints, message/data paths, and launch policy.

Fast Light similarly separates FLRig control, FLDigi modem/logs, shared
FLMsg/FLAmp tools, and their relationship. VarAC separates node-local paths and
connections from optional cluster-shared configuration.

When discovery cannot identify one executable unambiguously, `Choose
application...` presents the bounded detected candidates and always includes a
`Browse...` fallback. Browse selects an application; it does not force the
operator to invent recipe-owned profile, storage, message, or command fields.
Changing the application invalidates the prepared recipe and requires
repreparation before Continue or Save.

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
- instance source, ownership, variant, endpoint summary, and launch policy;
- whether FIO will create, use unchanged, share, or leave an application
  operator-managed;
- native configuration actions and operator-owned actions in plain language;
- receive/tune/QSY/PTT/transmit/scheduler permissions;
- Receiver Guard or RF Guard assignments and schedule result;
- VarAC node and optional cluster membership, with node-local and shared
  resources separated;
- conflicts, warnings, verification evidence, and recovery consequences; and
- the state that will remain if an optional live application is unavailable.

Review contains a visible compact **Launch plan** section. It lists every
external component, `Launch with FIO` or operator-start state, startup choice,
readiness state, and the distinction between known recipe, startup inclusion,
and bundle enabled. `Show details` for each family reveals exact paths, commands,
working directories, dependencies, execution scope, fingerprints, backup
targets, and exclusive claims. This technical evidence remains keyboard and
screen-reader accessible but is collapsed by default; it is not buried in a
generic Advanced editor. Required warnings, existing-object impacts, unsafe or
unknown state, and the reason Save is blocked remain visible.

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

The normal managed proposal derives the visible instance name and rig/profile
stem from the configured radio name. It places the new settings/profile and
application-data/message roots in a dedicated FIO-managed location for that
stable instance key. An existing JS8 settings file, application-data directory,
`DIRECTED.TXT`, TCP API port, or UDP port is never the default for a distinct
proposal. The application binary may be shared; the instance identity may not.

Before any correction or Files surface appears, the prepared plan already
contains and summarizes the new rig name, profile/configuration root, Qt
application-data root, `DIRECTED.TXT`, `ALL.TXT`, `inbox.db3`, TCP API, and
applicable UDP claims. It labels managed targets `FIO will create` when a
qualified writer owns the operation or `FIO will reserve; operator action
required` when it does not. Another radio's values appear only after the
operator explicitly chooses `Use an existing instance unchanged`.

FIO owns version- and platform-qualified launch recipes for stock JS8Call,
Improved, and Subspace. Each recipe defines the executable, exact rig/profile
selector arguments, working directory/environment, application-data semantics,
TCP and applicable UDP claims, readiness policy, and native-writer capability.
The normal workflow shows a concise `Launch with FIO` summary, not an empty
`Custom launch command` field. A custom command is Advanced-only. When no exact
recipe is qualified, FIO says `Operator setup required`, preserves the inactive
draft, and gives exact steps without pretending the instance is launch-ready.

The port allocator checks saved application records, manifests, launch bundles,
the current transaction, and family-internal conflicts. A new proposal may use
`2443` when `2442` belongs to an existing instance, but the entire new identity
must use the new proposal. Importing the `2442` instance imports its whole
bundle unchanged.

The inventory and allocation rule applies independently to every applicable
TCP and UDP endpoint; it is not limited to device-profile projections. Final
Save consumes the exact reviewed bundle fingerprint and inventory generation.
If either changed, Save fails closed before mutating an existing row, link,
manifest, native file, or launch bundle.

On an observer, FIO never issues JS8 transmit, Expect, retrieval, Compose, or
PTT actions. A managed native writer must also apply and verify the reviewed
receive-only settings it claims to own; otherwise Review identifies the exact
operator step and keeps the instance unverified.

### Fast Light

Fast Light receives the same create/import/manual lifecycle as JS8Call.

A transceiver Fast Light family contains radio-scoped FLRig and FLDigi
instances and their explicit control relationship. FLMsg and FLAmp installations
may be shared, but managed multi-radio runtime/data identity follows GRS-13:
FLMsg is radio-scoped by default, and FLAmp is radio-scoped only through a
qualified or adopted isolation recipe.

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

The operator-facing Fast Light instance name defaults to exactly the configured
radio name; the UI does not append `Fast Light`. Internally, the family contains
separate stable FLRig and FLDigi component identities. For a transceiver, a
qualified platform/version recipe supplies each component's profile folder,
endpoint, executable, exact arguments/working directory, launch order, and
readiness policy. For a receive-only SDR it supplies FLDigi only. A single
family-level command or a blank `optional launch command` is not a complete
managed proposal.

The normal flow displays the resolved launch summary. Per-component custom
commands and profile overrides are Advanced-only and appear only when the
operator chooses a custom/manual source or the installed version lacks a
qualified recipe. Reusing the FLRig/FLDigi executable is allowed; reusing an
existing radio's native profile folders, endpoints, or attributable log roots
is not the default for `Create a distinct instance`.

Preparation resolves and summarizes the FLRig/FLDigi configuration roots,
FLDigi log and check-in roots, endpoints, dependency order, commands, and launch
policy before any Files or correction surface appears. The qualified normal
path asks for no custom command or raw profile folder. It derives the
radio-scoped FLMsg NBEMS/auto roots and either a qualified radio-scoped FLAmp
identity or an accurately labeled shared/operator-managed limitation.

The transceiver-only Advanced option is labeled **Allow FIO to initiate Fast
Light transmissions**. Its explanation is: `Off by default. Needed only for
FIO-requested PTT, macros, queues, or automatic send; it does not affect
ordinary manual FLDigi use.` It remains subject to FIO Behavior, RF Guard, and
final preflight. The option never appears for a receive-only SDR.

Fast Light persistence distinguishes the station-shared installation from each
radio's runtime/data identities and from any explicitly shared station/group
service. Radio-scoped FLMsg roots, qualified FLAmp roots, FLRig/FLDigi
endpoints, native profiles, attributable logs, and check-in paths are
collision-checked resource claims. Deliberately shared resources carry an
explicit scope and owner rather than a non-exclusive path guess. FLRig precedes
its linked FLDigi; FLMsg or FLAmp depends on FLDigi only when the selected
integration requires it. No dependency is invented for an independent
receive-only file workflow.

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

VarAC source and **VarAC arrangement** are separate choices. Arrangement is
selected immediately after VarAC is chosen and before node, cluster, Files, or
connection details. The compact inventory summary explains why an arrangement
is recommended; discovery never silently selects a mutating cluster operation.

| Inventory state and source | Arrangement choices before Prepare | Initial state or recommendation | Preparation behavior |
| --- | --- | --- | --- |
| No nodes and no clusters; new managed node | Standalone; Create the first cluster | Standalone is preselected | Generate a node proposal; generate cluster resources only if Create is selected |
| Standalone node(s), no clusters; new managed node | Standalone; Create a cluster with a named existing standalone node and this radio; Import; Manual/remote | No mutating choice is preselected; Create cluster is marked **Recommended** | Preserve the existing node and prepare a new node plus proposed two-member cluster only after explicit selection |
| One or more valid clusters; new managed node | Standalone; Join a named existing cluster; Create a new cluster; Import; Manual/remote | Standalone is the safe initial choice; joining is never automatic | Prepare a distinct node and collision-free proposed membership for the selected named cluster |
| Import existing | The source-locked current arrangement | Explicit imported source | Create no new cluster operation; changing arrangement requires Clone as distinct |
| Manual/remote | Standalone/manual, or join only with explicit reviewed target evidence | Manual | Invent no command, native configuration, or membership evidence |

For the production-shaped case of one existing standalone node and no cluster,
the visible summary is:

`Existing setup: FTDX-10 VarAC is standalone. No VarAC cluster is configured.`

The recommended choice is:

`Create a cluster with FTDX-10 VarAC and <new radio> — Recommended`

Its Why text states: `Creates coordinated membership only after your final
review; it does not change FTDX-10 now.` The operator must select that choice;
recommendation styling is not consent. `Create another standalone VarAC node`
remains available. `Join an existing cluster` is absent or disabled with the
exact reason when no valid cluster exists.

After arrangement selection, Prepare generates only the relevant node-local and
cluster proposal. A managed plan shows intended launcher, working directory,
INI, database/runtime, incoming/inbox, and outbox targets as read-only facts. If
no qualified VarAC writer or launch recipe exists, it says **Manual VarAC
configuration required** and gives one precise next action; blank technical
fields are not presented as if the operator should know how to complete them.

The native cluster writer is invoked **only** for an explicit `Create cluster`
or `Join cluster` arrangement. `Standalone` must never be passed to that writer
and must never be rewritten as a one-member cluster. For standalone, FIO saves
the distinct radio/software identity, retains any discovered node-local paths,
marks native standalone files as operator-owned, and presents a non-blocking
warning. The VarAC arrangement selector remains visible on that warning so the
operator can intentionally change to Create or Join and trigger a new prepared
context. If Create/Join preparation is blocked, the selector likewise remains
visible and enabled beside the exact blocking reason; a blocked result may not
collapse or hide its only recovery control. Create/Join continues to require a
qualified, generation-current native bundle before Next or final Save.

Membership records cluster ID, unique positive instance number, effective shared
database, counter refresh, email-gateway sender, and PTT-lock policy. For
create-cluster using an existing standalone node, final Review names that node,
the new node, both proposed memberships and instance numbers, gateway choice,
PTT policy, node-local resources, and cluster-shared resources. One transaction
creates the new node and cluster, adds both memberships, updates the explicitly
reviewed existing-node relationship, saves the new radio link and launch plan,
and enables the reviewed email-gateway sender when that service is selected.
Cancel, Back, repreparation, stale evidence,
or any failure leaves the existing standalone node unchanged and creates no
cluster or membership. Standalone VarAC never implies cluster mode.

The cluster record owns normalized cluster ID, effective shared database,
counter refresh, email-gateway sender selection, and PTT-lock policy. Membership owns the cluster reference,
radio/node reference, enabled state, and a positive instance number unique among
enabled members of that cluster. Public cluster IDs are unique
case-insensitively after normalization. A native-managed cluster database is a
required non-exclusive resource owned by one cluster identity; every managed
member's effective VarAC database reference resolves to that cluster resource.
Standalone and operator-managed nodes retain their node-local database behavior.
The detailed native ownership and recovery contract is defined in
`varac_native_cluster_configuration_spec.md`.

Creating the first node completes cluster creation, adds and enables the node
membership, and only then permits that enabled member to be selected as the
email-gateway sender,
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

The FIO Spotter MCF catalog is a built-in FIO dependency, not a radio-scoped
external software instance. The normal flow resolves the packaged or configured
station MCF catalog automatically and does not ask for an `MCF Forms Folder`.
An optional custom catalog is Advanced station configuration; it is not copied
from a JS8 instance and creates no Spotter launch identity.

CommStat is one intentional station-shared process by default. Selecting
CommStat for another radio does not create a duplicate executable, profile, or
launch item. It creates or reviews a binding from that shared CommStat identity
to the exact radio-owned JS8 instance/endpoint. One CommStat launch item is
deduplicated by its durable shared identity, while each radio binding remains
visible and collision-checked. An observer binding is receive-only; no CommStat
mapping expands the capability of its radio or JS8 transport. If a future
CommStat version requires separate processes, that behavior requires an
explicit version-qualified contract rather than silent duplication.

### Existing-Instance Protection And Review

An imported existing instance is source-locked as one fingerprinted bundle.
Identity fields are read-only in the import path. If the operator changes a
profile, data root, endpoint, command, selector, or other identity field, the
workflow changes to `Clone as distinct`, allocates a new immutable identity and
resources, and leaves the source unchanged.

Review groups decisions by operator intent rather than exposing implementation
fields:

- **New instance for `<radio>`:** distinct identity, profile/data roots,
  endpoints, and resolved launch behavior;
- **Existing instance:** exact source owner and unchanged fingerprint;
- **Shared station service:** one service identity plus the new radio binding;
- **Operator action required:** the exact unsupported native step and safe
  inactive state.

Known launch recipes are shown as resolved facts. `Custom launch command`, raw
settings paths, and application-data roots are hidden from the normal scan path
unless FIO cannot qualify a recipe or the operator opens Advanced. Hiding these
fields never hides a collision, unverified writer, external action, or unsafe
capability.

## Native Configuration Writer Contract

FIO may create or update third-party native configuration only through an
application-, variant-, version-, and platform-qualified writer. There is no
generic best-effort writer.

Preparation may reserve and display intended managed roots, endpoints, launch
arguments, and native-file targets without writing them. The plan distinguishes
`FIO will create and verify at Save` from `FIO will reserve; operator action
required`. Only the first state is permitted when the exact qualified writer
below is available; proposing a path is never evidence that the native file
exists or is valid.

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

### Candidate classification and incomplete records

A **usable existing candidate** is one complete atomic bundle with a stable
identity, explicit ownership or source evidence, every required endpoint and
path for its family/source, and no unresolved exclusive-resource collision. An
enabled application row alone is not proof of a usable or FIO-managed instance.

An unlinked or incomplete application row is classified as **Orphaned or
incomplete record** when required ownership, manifest, launch, endpoint, path,
or provenance evidence is absent. Such records:

- appear only in a diagnostics/recovery view, never as the default existing-
  instance choice;
- never receive a launch item or radio assignment merely because their row is
  enabled;
- remain byte/row unchanged during discovery, preparation, Add Radio, Cancel,
  Back, or failed Save; and
- retain conservative resource claims until an explicit reviewed maintenance
  operation reconciles them.

The allocator normalizes claims by family, transport, host, port, profile/data
root, log root, database, and launch identity. Duplicate legacy rows claiming
the same resource produce one conservative collision claim rather than an
unbounded sequence of invented reservations. A claim also owned by a complete
linked instance remains reserved by that live identity. A new managed proposal
chooses the next collision-free complete bundle; it never repairs, silently
reuses, disables, relinks, renumbers, relocates, or deletes an orphan.

An empty `software_instance_manifests` table is supported. Discovery uses saved
configuration projections, application rows, launch bundles, configured paths,
and bounded native evidence, and labels provenance **Managed evidence
unavailable** or **Configuration provenance unverified** where appropriate. It
does not infer FIO management from an executable path. A successful new Save
creates the reviewed manifest atomically. Discovery never scans traffic,
message, ingest, link-history, sync-history, or message-projection tables to
compensate for absent manifests.

Cleanup is a separate explicit maintenance workflow with preview, backup or
recoverable disable/archive behavior, impact review, and its own acceptance
gate. This guided workflow does not perform cleanup.

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

The visible cache state uses calm, stable language: `Using cached station
inventory`, `Refreshing software inventory...`, `Updated`, or `Could not
refresh — using prior snapshot`. Publication preserves family/source choices,
prepared drafts, focus, scroll position, and detail expansion. An explicit
Prepare or `Find installed software` action may start work; ordinary navigation,
render, resize, or family selection alone does not start a new scan.

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
- Add Radio never queries `freqinout_nets.db` or any traffic/message/history
  table. Large operational history cannot affect preparation latency.
- Repeated Prepare, Back, and Next for the same current generation reuse the
  coordinator result and do not repeat profile scans or filesystem traversal.

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
and selected by default. Unattended launch when FIO opens has no runtime effect
until the radio is saved, runtime-active, opted into automatic launch, and
included in the station launch plan. An explicit row **Start** or **Start
Startup Apps** action may start the selected saved radio's reviewed recipes
while that radio is inactive; this prepares its applications without silently
making it the active or command-focus radio. Imported, manual, remote, and
shared station applications default to operator-start unless the operator
explicitly opts into FIO launch. Review always shows the effective choice.

Manual start and FIO startup call the same `StationLaunchPlanner`. Shared tools
are deduplicated only by an intentionally shared durable identity. Endpoint-
scoped applications are never deduplicated by process name alone. Dependency
examples include FLRig before its linked FLDigi and SDR++ readiness before a
dependent receive workflow when the operator has requested that ordering.

The radio-level automatic-start switch, the primary operating model's
unattended-start permission, each row's **Launch at Startup** selection, and an
explicit manual start are distinct gates. The first two apply only when FIO
opens. They must never disable row **Start** or **Start Startup Apps**. A manual
group start requires at least one checked startup row and no launch sequence
already in progress; a manual row start requires a valid selected-radio recipe
and no launch sequence already in progress.

Runtime status and already-running suppression are instance-scoped. For
applications that share one executable, FIO matches the selected radio's
canonical executable plus its complete profile/config/rig-name argument vector
(and structured Wine arguments where applicable). A same-family process for
another radio is not evidence that the selected instance is running. Endpoint
reachability remains a separate connected/not-connected result. While an exact
instance probe is pending, the UI shows a checking/unknown state and does not
substitute station-wide family status.

Canonical-to-projection parity is a review and repair signal, not a blanket
launch veto. A mismatch between a canonical software identity and its
Software Administration, manifest, or launch projection remains visible as
`Needs attention`, is logged with the affected family and fields, and is shown
with manual Launch Control feedback. FIO may continue using an exact reviewed
launch-bundle recipe when that recipe independently passes launch-time
validation. One family's projection warning must not suppress valid recipes
for the other selected families.

Only conditions that make execution unsafe or indeterminate block launch:
native-writer recovery that could expose an uncommitted external configuration,
malformed or absent executable/argument facts for the requested component,
resource/identity collisions, dependency cycles or failed required
dependencies, receive-only/transmit authority violations, or another launch
sequence already in progress. Endpoint unreachability before a requested start
is status evidence, not itself a reason to prevent starting that endpoint's
application.

`Launch at Startup`, `Monitor Health`, and the radio-level automatic-start
switch are operator-owned Launch Control preferences. Changing them after Add
Radio does not change the executable, profile, endpoint, or data identity and
must not create canonical recipe drift. Their durable source of truth is the
radio launch bundle.

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

The frozen review fingerprint includes every selected family source, the VarAC
arrangement and named existing node/cluster when applicable, recipe and
inventory generations, generated resources, native-writer capability/state,
and every reviewed existing-object impact. A source, topology, inventory, or
writer change requires repreparation and fails before FIO or native mutation.

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

Cancel releases only in-memory draft reservations. Existing complete and
orphaned/incomplete application rows, manifests, launch items, links, native
paths, and VarAC topology remain unchanged. A failed create-cluster transaction
also restores the existing standalone VarAC node and leaves zero new cluster or
membership rows.

If an unavoidable post-radio compatibility failure occurs on an older database,
the radio remains inactive and Review gives one exact resumable route. The UI
must not report the overall operation as successful.

## UI, Responsive, And Accessibility Contract

The guided surface has three geometry owners:

1. a fixed, non-scrolling header containing purpose, selected radio/context,
   current status, and responsive step chips;
2. exactly one vertical `QScrollArea` containing the current step body; and
3. a fixed, non-scrolling footer containing `Cancel`, `Back`, and one
   state-dependent primary verb such as `Prepare selected software
   automatically`, `Continue`, `Apply to radio draft`, or `Save Radio and
   Software`.

Ordinary forms and Review do not create a second same-axis scroll owner. A
deliberately bounded list, log, or code-like technical value may own local
overflow without taking over page navigation. Expanding or collapsing details
never moves the footer offscreen.

The surface bounds derive from the available screen work area, active font
metrics, and shared spacing/control helpers; a fixed `780x700`-style assumption
is prohibited. At 1920x1080, 1000x700, and 900x560 in Normal and Large Text,
light and dark themes:

- steps wrap without truncation and retain stable order;
- compact mode presents one decision in a readable vertical sequence;
- buttons wrap or stack before text is clipped;
- primary content has no page-level horizontal scrollbar;
- the current task and fixed action footer remain reachable;
- app cards stack while preserving reading order;
- labels and evidence grow or wrap without overlap;
- status publication does not rebuild the page, reset selection/focus/scroll or
  disclosure state, produce geometry oscillation, or cause swipe-and-vanish;
- status is communicated by semantic text and icon as well as shared themed
  color; and
- application, radio, profile, endpoint, and action labels never use clipped
  ellipses when the distinction affects ownership or safety.

`Show details` / `Hide details` is family-scoped, keyboard-focusable, preserves
scroll position, and has an accessible name/description naming the family and
radio. It may hide raw paths, commands, dependencies, fingerprints, and
diagnostics. It never hides the target radio, selected source or VarAC
arrangement, safety or blocked reason, required confirmation, unsaved state,
existing-object impact, or destructive consequence.

All visual treatment comes from the shared theme and component helpers. There
is no screen-local palette, font, text-bearing control height, focus treatment,
or imitation chip/card style. Help and tooltips belong to the specific label or
action; there is no master tooltip over a section.

Discovery and endpoint status are inline. Normal correction stays inside the
guided page or its current drawer/stacked task. A top-level modal is reserved
for destructive replacement/role-change confirmation or a failure that cannot
be represented in the owning card. Opening full standalone Software
Administration is an explicit secondary route after preparation, not the normal
continuation.

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
7. With an existing JS8 bundle on one TCP/UDP pair and a retained unsaved draft
   on the next pair, Add Radio `Create a distinct instance` proposes the next
   collision-free pair plus a new stable key, rig selector, profile/data roots,
   message files, manifest, and launch identity. It does not select either
   existing profile.
8. Back/Next, discovery refresh, and radio display-name edits do not change a
   reviewed draft identity. A stale inventory generation or altered bundle
   fingerprint blocks Save before any mutation.
9. Import shows a source-locked JS8 or Fast Light bundle. Editing an identity
   field requires `Clone as distinct`; Cancel and Save never mutate or hybridize
   the source application row or manifest.
10. Production Add Radio and Software Administration use the same proposal and
    inventory coordinator; no loose-field draft path can bypass atomic bundle
    validation.

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
8. A transceiver's Advanced **Allow FIO to initiate Fast Light transmissions**
   acknowledgement is durable and review-visible, while role, endpoint,
   profile, or RF Guard changes require final-preflight revalidation. A stale
   or injected observer acknowledgement is rejected.
9. Case-insensitive duplicate VarAC cluster IDs and duplicate enabled member
   numbers fail before mutation; cluster-shared and node-local databases cannot
   be silently exchanged.
10. With one standalone VarAC node and no cluster, Add Radio shows the exact
    topology, marks create-cluster-with-that-node as Recommended without
    selecting it, retains standalone as an explicit alternative, and creates no
    cluster or membership until final reviewed Save.
11. A managed Fast Light proposal produces separate FLRig and FLDigi component
    identities, recipes, roots, endpoints, and readiness evidence. The visible
    family name is the radio name without an appended `Fast Light` suffix.

### Launch and platform behavior

1. Every selected external app appears exactly once in Review with a concise
   launch policy or explicit operator-start state; `Show details` exposes the
   exact effective command.
2. Manual and startup launch plans use the same recipes and ordering. Manual
   selected-radio scope may include an inactive saved radio; unattended startup
   includes only runtime-active radios whose automatic gate is enabled.
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
8. Known JS8 and Fast Light recipes require no custom-command input in the
   normal path. Each generated command's selector, working directory, and data
   root match the reviewed atomic claims exactly.
9. With two radios using the same FLRig, FLDigi, FLMsg, JS8Call, or VarAC
   executable, a process whose canonical launch arguments belong to radio A
   cannot mark radio B running and cannot suppress radio B's explicit launch.
10. Disabling the primary operating model's unattended launch permission or a
    radio's automatic-start switch leaves the selected radio's explicit row
    **Start** and **Start Startup Apps** actions available.
11. Canonical projection drift produces a visible review warning but does not
    remove an independently valid launch-bundle recipe from manual or startup
    planning; genuinely unsafe launch-time conflicts remain blocking.
12. Editing `Launch at Startup` or `Monitor Health` does not create canonical
    software-identity drift and does not require rebuilding the application
    identity.
13. Station custom-tool catalog edits are lossless with respect to every
    radio-owned launch row. They cannot remove a canonical VARA/SDR++ companion,
    erase FLRig/FLDigi/JS8Call/VarAC selectors or working directories, or copy
    one radio's monitoring/startup choices into another radio.
14. Each rendered Launch Control row binds to its immutable component/instance
    key. A radio-focus transition must complete the old-radio stash before the
    selected ID changes and must reject table-to-cache synchronization until
    the table has been repainted for the selected radio.
15. A legacy row that lost canonical recipe fields is recoverable from that
    radio's committed identity plus manifest. Recovery replaces only canonical
    recipe fields and preserves the radio-owned enabled, startup, and monitor
    preferences; no cross-radio or process-name inference is permitted.
16. One station-shared CommStat process can bind to multiple distinct radio-owned
   JS8 endpoints. It launches once, shows every binding, and is never
   deduplicated or reassigned solely by process name.

### Existing-station UX and logical defaults

1. Add Radio uses the operator-facing role labels `Transceiver` and
   `Receive-only SDR` consistently; no redundant `(receive-only)` suffix is
   appended.
2. Step 2 presents FIO behavior, not a schedule. Protected defaults are
   `Standard transceiver operations` and `Receive-only monitoring`; schedule
   names appear only in Step 6.
3. Selecting built-in FIO Spotter resolves the station MCF catalog without a
   normal-flow folder question and creates no external launch item.
4. Selecting CommStat on an additional radio offers the existing shared service
   and the new JS8 binding rather than a duplicate CommStat instance.
5. `Create a distinct instance` may reuse an installed executable but never
   preselects another radio's profile, settings file, data/message root,
   endpoint, or launch identity.

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

### GRS-7 prepare-first and production-shaped remediation

1. The exact route `Settings -> Radios -> Add Radio -> TriMode -> Transceiver`
   shows software/source choices, applicable VarAC arrangement, and `Prepare
   selected software automatically` before any administration, Files, raw path,
   port, command, profile, database, or folder editor.
2. With one complete linked JS8Call bundle on API `2442`, preparation creates a
   new rig name, profile/data roots, `DIRECTED.TXT`, `ALL.TXT`, `inbox.db3`, and
   unique TCP/UDP claims. It never displays or uses the existing profile unless
   `Use an existing instance unchanged` is selected.
3. A qualified JS8Call or Fast Light managed plan requires no raw Files or
   custom-command input. A missing or ambiguous executable offers bounded
   detected candidates plus `Browse...`; the operator is never stranded in a
   blank form.
4. The production-shaped fixture contains one linked complete JS8Call/Fast
   Light/VarAC set, seven unlinked incomplete JS8Call rows with duplicate API
   claims, seven unlinked incomplete Fast Light rows with duplicate endpoints,
   and no manifests. Incomplete rows are diagnostic-only candidates, remain
   unchanged, and contribute conservative normalized collision claims without
   causing repeated scans or unbounded port increments.
5. Empty-manifest discovery completes from bounded configuration evidence,
   labels provenance unverified, never queries operational history, and creates
   a new reviewed manifest only within successful final Save.
6. FIO Spotter asks no catalog/folder/launch question. CommStat creates only one
   binding to the new radio-owned JS8 endpoint and does not duplicate the
   station service.
7. With exactly one standalone `FTDX-10 VarAC` node and no clusters, the
   production summary and create-cluster recommendation use the exact contract
   copy, no mutating choice is preselected, and cluster/node detail fields do not
   appear before arrangement selection and preparation.
8. Choosing the standalone alternative creates no cluster rows. Choosing the
   recommended create-cluster path reviews both members and commits new node,
   cluster, memberships, gateway/PTT policy, launch plan, and radio link in one
   transaction. Cancel, stale generation, duplicate member number, or injected
   failure leaves the existing node unchanged with zero new cluster/membership
   rows.
9. Existing clusters produce named Join choices and the next collision-free
   member proposal. Import remains source-locked. Manual/remote mode invents no
   native configuration or launch command.
10. Without a qualified VarAC writer/recipe, the plan shows `Manual VarAC
    configuration required` and one precise next action instead of blank INI,
    database, inbox, outbox, working-directory, or launcher fields.
11. `Show details` starts collapsed, is family-scoped and keyboard accessible,
    reveals exact technical evidence, preserves focus/scroll/expanded state
    through refresh and resize, and never hides safety, Why, confirmation,
    unsaved state, or existing-object impact.
12. Real-widget and screenshot-shaped checks at 1920x1080, 1000x700, and
    900x560 in Normal/Large Text and Light/Dark themes prove one body scroll
    owner, fixed reachable footer, stable step order, no clipped/overlapping
    control, no page-level horizontal scroll, and no swipe-and-vanish reflow.
13. Preparation renders the cached snapshot immediately, starts at most one
    explicit bounded worker per generation, reports progress/cancel within 250
    ms, keeps navigation and typing responsive, and fences stale publication.
    Repeated Prepare/Back/Next does not repeat the same filesystem/profile scan.
14. The final frozen plan includes source, arrangement, recipe/inventory
    generation, native-writer state, generated claims, and existing-object
    impacts. Any mismatch fails before FIO/native mutation. Cancel and every
    injected failure preserve byte/row-level equality for all pre-existing rows
    and native targets.

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

Status: pure-model automated gate passed 2026-09-17; production Add Radio
integration is reopened by GRS-6. The Qt-free coordinator, scanner adapters,
and pure proposal planner are `guided_software_discovery.py`,
`guided_software_discovery_sources.py`, and `guided_software_proposals.py`.
Settings Add Radio and explicit Software Auto-Fill share the coordinator and
perform discovery only from worker threads. This slice changes no runtime
schema, native application configuration, endpoint, radio, or ownership.

- Replace redundant Add Radio scans with one coordinator and immutable cache.
- Produce complete JS8, Fast Light, receiver, and VarAC candidates/proposals.
- Add structured timing/cancellation telemetry and deterministic port/resource
  planning.

Original automated exit: bounded/cancel/stale-result/performance tests passed;
no discovery writes or GUI-thread I/O occurred. The pure proposal prevents the
`2443`/existing-profile regression, but the 2026-09-17 live run proved Add Radio
could bypass that proposal. GRS-6 must close the production-route gap.

### GRS-2 — Unified Guided UX

Status: automated UX gate passed 2026-09-17; distinct-instance production
integration is reopened by GRS-6. Add Radio uses the stable seven-position
workflow for both transceivers and observers, adapts Guard and Schedule content
without hiding steps, and opens the Software Instance Assistant against an
opaque unsaved-radio owner key rather than a fake persisted ID. The live run
showed that this handoff did not include the authoritative existing-instance
inventory or atomic proposal. Cancel remains a no-write boundary.

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

The 2026-09-17 transceiver run did not pass this live exit. `Create a distinct
instance` offered an existing JS8 profile/port bundle, Software Administration
showed existing JS8 settings/data paths, Fast Light required confusing manual
identity/launch input, CommStat appeared duplicative, FIO Spotter exposed an MCF
folder that should be resolved by FIO, and existing VarAC made cluster intent
ambiguous. The operator canceled; no successful configuration claim is made.

### GRS-6 — Existing-Station Distinct-Instance Remediation

Status: automated implementation passed 2026-09-17. GRS-6.1 through GRS-6.5
passed their automated exit gates; the repeated operator-assisted live route
remains a release blocker. This reopens the affected GRS-1 through GRS-4
integration claims without discarding their valid pure-model and safety work.

GRS-6 proceeds sequentially:

1. **Authority and inventory:** connect Add Radio and Software Administration
   to one immutable inventory/proposal coordinator; create a stable draft key;
   lock imports; reject loose-field hybrid persistence.
2. **JS8 and Fast Light recipes:** generate complete radio-name-derived,
   collision-free per-platform identities, roots, TCP/UDP endpoints, component
   commands, dependencies, and launch summaries. Known recipes require no
   normal-flow custom command.
3. **Supporting-family decisions:** resolve built-in Spotter MCF catalog;
   implement one shared CommStat identity with per-JS8 bindings; use the then-
   approved safe standalone VarAC default and keep cluster create/join explicit.
   GRS-7 supersedes only that default-selection policy with the conditional
   arrangement matrix above.
4. **Task-oriented UX:** separate Radio Role from FIO Behavior, use the approved
   labels, show resolved choices and Why, and keep raw paths/commands in
   Advanced or exact recovery guidance.
5. **Transactional and live qualification:** prove final Save consumes the
   reviewed fingerprint/generation, fault injection preserves existing native
   and FIO configuration, and repeat the reported TriMode path against an
   already-configured station on the current macOS worktree before broader
   platform gates.

#### GRS-6.1 Exit Evidence — Authority And Inventory

Passed 2026-09-17. Add Radio and Software Administration now consume the same
immutable saved-plus-retained inventory contract. A new draft receives an
opaque identity derived from the setup transaction and family, not from an
editable radio or instance label. Retained drafts reserve their endpoints in
the same collision view used by later proposals.

An imported instance is a source-locked complete identity. Its endpoints,
profile/configuration roots, data/message roots, and launch identity are not
editable in place; `Clone as a distinct instance` returns to the core distinct
proposal path and retains only safe executable/version evidence. Both
production persistence routes re-read the selected source and reject a changed
or missing source fingerprint before mutation. The atomic store remains the
last collision authority and rolls back application, manifest, launch, and
radio-link changes together.

The focused authority/inventory, assistant, Add Radio, Software Administration,
settings adapter, observer, responsiveness, and adjacent regression partition
passes. GRS-6.1 does not claim that dedicated JS8/Fast Light native roots and
qualified launch recipes are complete; those are the GRS-6.2 exit gate.

#### GRS-6.2 Exit Evidence — JS8 And Fast Light Recipes

Historical note: GRS-13 supersedes the managed-root placement and generic
station-shared FLMsg/FLAmp policy recorded in this 2026-09-17 evidence. The
test result remains historical evidence only.

Passed 2026-09-17. Add Radio and Software Administration now resolve managed
software from FIO's configuration-owned `managed-instances` root rather than
the operator's executable-search folder. JS8Call recipes use the stable draft
identity for the profile and `--rig-name`, allocate distinct TCP and UDP
claims, and derive the actual platform/rig-specific Qt application-data root
used by `DIRECTED.TXT`, `ALL.TXT`, and `inbox.db3`. The recipe root, save/forms
roots, and native profile planner agree. A blank managed root fails closed.

Stock 2.2.0, Improved 3.0.3, and Subspace 4.1.0.478 are exact qualified JS8
contracts; another variant/version remains inactive with an explicit Advanced
operator route. Fast Light resolves separate FLRig and FLDigi configuration
roots, endpoints, launch arguments, readiness checks, and dependency order.
An observer receives an FLDigi-only receive-scope recipe. FLMsg and FLAmp remain
explicit station-shared components rather than disappearing from launch review.

Qualified recipes hide recipe-owned profile/data fields and custom-command
inputs from the normal flow while displaying exact component commands, roots,
endpoints, dependencies, readiness, execution scope, and startup policy.
Persistence projects that same reviewed recipe into component launch rows;
unsupported recipes never invent a command or relative path. The automated
GRS-0 through GRS-6.2 integration partition passes 479 tests with eight explicit
live-gate skips; compilation and diff checks pass. Live native application
behavior remains part of GRS-6.5 and is not inferred from these tests.

#### GRS-6.3 Exit Evidence — Supporting-Family Decisions

Passed 2026-09-17. FIO now packages the complete 30-form station Spotter
catalog and resolves it whenever an optional Advanced custom catalog is blank,
missing, or invalid. Add Radio and Software Administration no longer present a
per-radio MCF folder or Spotter install/launch identity. Compose, receive
decode, FIO Spotter catalog mapping, JS8 NCS, and SOP consumers all use the same
resolver. Packaging metadata and the frozen-app specification include the
catalog.

CommStat remains one station-shared process identity. Each selected radio
persists a distinct binding to its own JS8 instance and endpoint, while launch
rows use the constant `commstat:station-shared` identity and inherit the one
reviewed station launch target. The station launch planner therefore starts at
most one process and retains all participating radio bindings. Disabling one
radio binding removes only that row and leaves other bindings intact.

At this historical gate, a new VarAC instance defaulted to **Standalone VarAC**
even when an existing node or cluster was discovered. Managed standalone
creation, import, and manual selection were explicit setup choices; Create
cluster and Join cluster were separate opt-in choices. GRS-7 supersedes the
default-selection behavior with the conditional, mode-before-details matrix
while retaining the rule that discovery never silently selects membership.

The GRS-6.3 supporting-family, persistence, launch, Add Radio, Software
Administration, guided/settings, Spotter ingest/compose/NCS/SOP, and adjacent
partitions pass. A monolithic Qt pytest process remains unsuitable as a release
gate because unrelated GUI/background-thread suites can abort when combined;
the same affected tests pass in isolated project-standard partitions. No native
application profile, runtime endpoint, radio, commit, or remote repository was
changed by the automated gate.

#### GRS-6.4 Exit Evidence — Task-Oriented Role And Behavior UX

Passed 2026-09-17. Fresh Add Radio exposes exactly **Transceiver** and
**Receive-only SDR** as hardware roles. A retained legacy gateway value is
preserved through a compatibility-only item when an existing profile is edited;
it is never offered for a new radio and is not coerced to a transceiver during
save.

Step 2 is now **FIO Behavior**. The protected normal choices are presented as
**Standard transceiver operations** and **Receive-only monitoring**, even when
an existing database retains an older generated display name. Custom behavior
names and durable Operating Model IDs remain unchanged. Capability and concise
Why summaries explain the selected feature/safety boundary; schedule timing and
Frequency Plan selection remain solely in the Schedule step. Redundant
`(receive-only)` suffixes were removed from guided behavior and schedule
choices.

For a qualified FIO-managed JS8Call or Fast Light recipe, the normal
Connections page shows the endpoint and a resolved summary but hides
recipe-owned executable, profile, data, and custom-command fields. Review shows
the exact effective component commands, working directories, dependencies,
configuration/data roots, endpoints, execution scope, and recovery route.
Unknown recipes still fail closed and expose their explicit Advanced recovery
path.

The primary core/migration partition passes 126 tests, built-in behavior/store
checks pass 15 tests, and the independently rerun GRS-6.4 language, real-widget,
responsive guided/settings, SDR, and assistant partition passes 79 tests.
Changed Python modules compile and `git diff --check` passes. No runtime
database, native application profile, external process, endpoint, radio,
commit, or remote repository was changed by this gate.

#### GRS-6.5 Exit Evidence — Transactional Save And Qualification

Automated implementation passed 2026-09-17. Add/Edit Radio now carries the
reviewed inventory generation and full fingerprint from Review into Final
Save. Software Administration carries the same evidence for a single-family
instance review. Both production routes rebuild current durable inventory
immediately before native work and again when an asynchronous native result
returns. A mismatch fails before database mutation, restores any completed
native write, and presents a task-oriented Software/Review recovery route.

One outer SQLite transaction now owns the radio profile, FIO Behavior
assignment, managed application rows, manifests, launch components,
CommStat/radio binding, schedule assignment, runtime activation, and
receive-only launch bundle. Existing store helpers retain their public
contracts, but their nested commits and rollbacks are deferred to the outer
transaction on the GUI thread. Final Save commits only after every required
step succeeds. An early return, validation failure, injected exception,
schedule/activation failure, receiver-launch failure, or database commit
failure rolls back the complete FIO change set. If qualified native files were
already applied, the existing backup/restore worker is invoked as part of the
failure route. No schema or destructive data migration was required.

Review shows a compact inventory generation/reference rather than raw internal
state. Save is single-activation: the button disables before the payload is
handed to the transaction owner, an indeterminate progress card is painted,
and a second click cannot submit a duplicate apply. Deselected software drafts
are removed from the final reviewed fingerprint so the save does not reject
its own current review.

Acceptance evidence includes stable/current-versus-stale fingerprint tests,
cancellation purity, no duplicate asynchronous apply, native failure recovery,
explicit-completion transactions, injected rollback after radio/software/
manifest/launch writes, receiver-launch rollback, narrow and large-font UI,
and adjacent guided/store/settings regressions. The focused GRS-6.5 tests plus
the Settings persistence adapter pass 27 tests; the combined guided integration
partition passes 293 tests,
and the selected 56-file guided/multi-rig/receiver/performance surface contains
789 passing tests when run in project-standard isolated processes. The focused
responsiveness/performance partition passes 75 tests. Python compilation and
`git diff --check` pass.

The monolithic GUI collection still reproduces the known Qt/background-thread
process abort; the same files pass in fresh processes and no GRS-6.5 failure
remains. Automated tests use temporary state only and did not change the
operator's runtime database, third-party profiles, applications, endpoints,
radio, git history, or remote repository. The exact TriMode/current-station
route below remains an explicit operator-assisted live release gate; this
document does not infer live application behavior from automated tests.

The historical GRS-6 automated exit required all existing guided suites plus
production-route tests for the acceptance items added above, responsiveness
instrumentation, fault-injection rollback, `compileall`, and `git diff --check`.
Its live exit route was:

`Settings -> Radios -> Add Radio -> TriMode -> Transceiver -> Fast Light + JS8Call + FIO Spotter + CommStat + VarAC -> Configure Automatically -> Software Administration -> Review`

That live evidence must show a new JS8 profile/data identity and non-conflicting
TCP/UDP endpoints, resolved Fast Light component recipes, built-in MCF catalog,
one shared CommStat binding, standalone VarAC unless cluster is explicitly
chosen, exact launch summaries, responsive navigation, and no mutation of the
existing station configuration before final reviewed Save.

### GRS-7 — Prepared Software Plan And Production-Shaped Remediation

Status: specified 2026-09-17; implementation and operator-assisted live
qualification are pending. Historical GRS-6 automated evidence remains valid,
but it does not satisfy this newly clarified operator sequence or the
production-shaped incomplete-record gate.

GRS-7 proceeds sequentially:

1. **Prepare-first sequence:** make software/source and VarAC-arrangement intent
   the only choices before `Prepare selected software automatically`; render the
   compact plan before any correction or administration surface.
2. **Inventory classification:** classify complete usable candidates separately
   from incomplete/orphan records, support empty manifests, normalize duplicate
   conservative claims, and prevent diagnostic rows from becoming recommended
   instances or launch items.
3. **VarAC topology:** implement the conditional arrangement matrix, including
   the explicit existing-standalone-to-new-cluster transaction and its
   non-mutating cancel/failure behavior.
4. **Progressive disclosure and layout:** consume prepared plans in the shared
   assistant, collapse technical evidence, use one body scroll owner with fixed
   header/footer, and preserve state across asynchronous publication.
5. **Qualification:** run the complete GRS-7 acceptance matrix, production-
   shaped read-only fixture checks, fault-injection, performance/lifecycle,
   responsive/theme/accessibility partitions, and the exact live route below.

Each numbered package is an exit gate; the next does not begin until the prior
package passes. No package may delete, disable, relink, or migrate existing
incomplete records. A cleanup tool, if later requested, requires a separate
specification and explicit operator authorization.

The GRS-7 live route is:

`Settings -> Radios -> Add Radio -> TriMode -> Transceiver -> choose Fast Light + JS8Call + FIO Spotter + CommStat + VarAC -> choose each source -> choose the VarAC arrangement -> Prepare selected software automatically -> review only Needs attention cards or optionally open Software Administration -> Connections -> Safety -> Schedule -> Review & Save`

The live gate must prove that preparation precedes technical correction, the
new JS8 and Fast Light bundles are complete and collision-free, FIO Spotter and
CommStat retain their built-in/shared contracts, the VarAC recommendation is
conditional and non-mutating until final Save, details begin collapsed, the
footer remains reachable, and cancel/failure changes neither the configured
station nor incomplete legacy records.

#### GRS-7.1 Exit Evidence — Prepare-First Sequence

Status: automated exit gate passed 2026-09-17. Inventory classification,
conditional VarAC recommendations, shared-assistant progressive disclosure,
and the complete operator-assisted live route remain open under GRS-7.2 through
GRS-7.5.

The Add Radio Software step now presents capability selection and the primary
`Prepare selected software automatically` action before per-family technical
correction. Selected families expose only source intent, and VarAC additionally
exposes non-mutating arrangement intent, until a prepared plan exists. A
successful preparation stays on Software and reports compact `Ready` or `Needs
attention` state; background work reports `Discovery in progress`; any family,
source, role, setup-type, or VarAC-arrangement change invalidates the plan as
`Stale — reprepare required`. Family correction actions and technical policy
controls remain unavailable until preparation succeeds. Correction dialogs use
task-specific `<family> setup for <radio>` titles rather than a generic
Software Administration handoff.

The existing generation/revision fence and dialog-close cancellation contract
remain authoritative. This package changes no schema, persistence path, native
application writer, software-instance ownership, launch behavior, existing
radio configuration, or VarAC topology. The conditional VarAC arrangement
matrix remains owned by GRS-7.3; the current control records intent only.

Model ownership and review:

- Primary `gpt-5.6-sol`, high reasoning: package boundary, lifecycle and stale-
  publication review, delegated-diff integration, acceptance execution, and
  specification/work-log reconciliation.
- `gpt-5.6-terra`, high reasoning: bounded Add Radio prepare-first UI, compact
  state presentation, plan invalidation, task-specific correction titles, and
  compatible public-contract test updates.
- `gpt-5.6-luna`, high reasoning: focused ordering, collapsed-details,
  Back/Next preservation, Cancel purity, and asynchronous generation-fence
  tests.

Acceptance evidence: changed Python compiles; `git diff --check` passes; and
the independent focused partition covering the new GRS-7.1 tests, unified
guided UX, supporting families, asynchronous autofill, performance boundaries,
and radio-scoped software settings passes **189 tests**. Tests used temporary
configuration state. No runtime database, production fixture, third-party
profile, process, endpoint, radio, commit, or remote repository was changed.

#### GRS-7.2 Exit Evidence — Inventory Classification

Status: automated exit gate passed 2026-09-17. VarAC topology, full shared-
assistant progressive disclosure/layout, and operator-assisted qualification
remain open under GRS-7.3 through GRS-7.5.

One immutable inventory classifier now separates `usable_existing`,
`recovery_only`, `diagnostic_only`, and `retained_draft` rows. It consumes
already-loaded durable radio links and manifest/source evidence; `enabled` and
an executable path are never ownership proof. A linked complete row remains a
usable existing candidate when the manifest table is empty, but the UI labels
it `Configuration provenance unverified` and does not claim native ownership.
A complete, source-evidenced unassigned bundle appears only in the operator's
explicit Find/import recovery picker. Incomplete or provenance-unknown rows
remain visible as disabled diagnostics with classifier reasons and cannot be
imported, recommended, assigned, source-locked, or launched by this workflow.

Endpoint and path claims are normalized into immutable sets. Duplicate legacy
rows therefore retain one conservative claim with a diagnostic duplicate count
rather than forcing repeated scans or one invented port increment per duplicate
row. Retained unsaved drafts reserve claims without becoming existing or
diagnostic candidates. Add Radio startup, standalone Software Administration,
bounded native discovery, and pre-Save revalidation now project the same link,
manifest, classification, and fingerprint evidence. Empty-manifest handling
reads only saved configuration/application/link evidence; no message, traffic,
ingest, sync, or operational-history table is queried.

Model ownership and review:

- Primary `gpt-5.6-sol`, high reasoning: classification/source-evidence
  architecture, Settings integration across initial/discovery/pre-Save paths,
  explicit recovery distinction, delegated-diff review, regression correction,
  gate execution, and specification/work-log reconciliation.
- `gpt-5.6-luna`, high reasoning: immutable core classification, normalized
  conservative claims, bounded production-shaped fixtures, empty-manifest and
  retained-draft tests.
- `gpt-5.6-terra`, high reasoning: disabled diagnostic presentation, explicit
  recovery-only picker presentation, provenance warning, accessibility, and
  focused real-widget tests.

Acceptance evidence: changed Python modules compile; `git diff --check` passes;
and the integrated GRS-7.2 inventory/UI, GRS-6 authority/recipe/transaction,
Software Administration, async discovery, prepare-first, and performance
partition passes **105 tests**. Synthetic fixtures reproduce one linked complete
bundle, seven incomplete unlinked duplicate claimants, and no manifests without
reading or changing the production database. No row, schema, cleanup state,
native profile, process, endpoint, radio, commit, or remote repository was
changed.

#### GRS-7.3 Exit Evidence — Conditional VarAC Topology

Status: automated exit gate passed 2026-09-17. Final operator-assisted
qualification remains open under GRS-7.5.

One pure snapshot adapter now consumes the already-loaded GRS-7.2 classified
VarAC rows, durable radio links, clusters, and memberships. It emits the
display-ready existing-setup summary, explicit arrangement choices, durable
node/device IDs, and collision-free member proposals without opening a store or
reconstructing incomplete native paths. Diagnostic and recovery-only rows
cannot affect topology. A fresh station safely defaults to standalone. Existing
clusters retain standalone as the safe default and expose named Join choices
with the next free enabled member number. One standalone node with no cluster
shows the exact named create-cluster recommendation but leaves the selector
blank; multiple standalone nodes require an explicit named member choice.

Add Radio presents that immutable recommendation before preparation and carries
only the selected core metadata through the shared assistant. Technical cluster
fields remain hidden until arrangement selection and preparation. The metadata
survives assistant review and radio-name refresh, is cleared when the operator
returns to standalone, and is accepted by final persistence only for an
explicit create-cluster route.

The existing-standalone create route now commits the new VarAC node, cluster,
existing member 1, new member 2, reviewed gateway/PTT policy, manifest, launch
plan, and new radio link in one SQLite transaction. The store revalidates the
existing durable link and absence of any prior membership under `BEGIN
IMMEDIATE`. Duplicate member numbers, stale assignment, observer membership,
missing node/link, and injected failure roll back every new node, manifest,
cluster, membership, launch, and link change while preserving the original
standalone node and radio assignment.

Model ownership and review:

- Primary `gpt-5.6-sol`, high reasoning: conditional-matrix architecture,
  display/persistence contract review, explicit member-number ownership,
  delegated-diff integration, transaction and failure-semantics review, exit-
  gate execution, and specification/work-log reconciliation.
- `gpt-5.6-luna`, high reasoning: pure recommendation/snapshot adapter, atomic
  store extension, duplicate/stale/fault-injection tests, and focused core
  validation.
- `gpt-5.6-terra`, high reasoning: bounded Add Radio presentation, exact copy,
  named choices, assistant metadata preservation, refresh/route-change behavior,
  and real-widget tests.

Acceptance evidence: the focused GRS-7.3, GRS-7.2, prepare-first, assistant,
guided VarAC, and store partition passes **63 tests**; the independently rerun
adjacent family, manifest, supporting-family, launch-recipe, radio-scoped,
Software Administration, unified UX, and performance partition passes **250
tests**. Changed Python modules compile and `git diff --check` passes. Tests used
temporary state. No schema, migration, cleanup, production database, native
profile, external process, endpoint, radio, commit, or remote repository was
changed.

#### GRS-7.4 Exit Evidence — Progressive Disclosure And Responsive Layout

Status: automated exit gate passed 2026-09-17. Final production-shaped and
operator-assisted qualification remains open under GRS-7.5.

Add Radio and the shared Software Instance Assistant now each use one vertical
body scroll owner surrounded by a fixed purpose/step header and a fixed action
footer. The Add Radio Back/Next actions live with Cancel and Save in the footer;
the seven stable steps remain outside the body scroll. Dialog bounds continue
to derive from the available work area rather than a fixed desktop assumption.

The shared assistant consumes prepared state as compact family, source, radio,
endpoint, launch, readiness, Why, safety, unsaved-state, and existing-impact
facts. Exact paths, commands, dependencies, fingerprints, and diagnostics begin
collapsed behind a family/radio-scoped keyboard-accessible `Show details`
control. Disclosure, focus, and per-step scroll position survive refresh,
navigation, resize, theme changes, and asynchronous publication. Qualified
managed recipes resolve before the Files page is painted, so generated JS8Call
and Fast Light roots appear as read-only prepared facts rather than blank path
prompts.

Primary integration review also closed three intent/identity gaps exposed by
the production sequence. A `Create a new FIO-managed instance` request may
reuse a qualified executable but cannot auto-select an existing JS8Call profile
or VarAC node-local files; switching from existing to create clears the borrowed
bundle. Add Radio uses exactly the operator's radio name for the visible
software instance. An explicit create-cluster arrangement receives one core-
generated, collision-free cluster name and public ID, and contradictory VarAC
gateway policies fail before any write.

Model ownership and review:

- Primary `gpt-5.6-sol`, high reasoning: prepared-state/intent architecture,
  Add Radio fixed-layout integration, generated VarAC cluster identity,
  create-versus-existing safety correction, delegated-diff review, acceptance
  execution, and specification/work-log reconciliation.
- `gpt-5.6-terra`, high reasoning: shared-assistant single-scroll layout,
  compact prepared facts, progressive disclosure, fixed footer, and focus/
  scroll preservation.
- `gpt-5.6-luna`, high reasoning: real-widget disclosure, safety/impact,
  responsive light/dark and Normal/Large Text matrix, accessibility, and state-
  preservation tests.

Acceptance evidence: changed Python modules compile and `git diff --check`
passes. The focused GRS-7.4, prepare-first, inventory, topology, autofill,
assistant, recipe, and unified Add Radio partition passes **101 tests**. The
independent adjacent family, persistence, Software Administration, Settings
adapter, launch-recipe, performance-boundary, and guided-setup partition passes
**328 tests**. Tests use temporary state and changed no schema, production
database, native profile, application process, endpoint, radio, commit, or
remote repository.

#### GRS-7.5 Exit Evidence — Production-Shaped And Final Automated Qualification

Status: automated gate passed 2026-09-17. The external application, hardware,
platform, and final operator-assisted Save/Cancel route remains open and blocks
the release claim. Automated evidence is not represented as live qualification.

The exact real-widget route now covers `Settings -> Radios -> Add Radio ->
TriMode -> Transceiver -> Software` with Fast Light, JS8Call, FIO Spotter,
CommStat, and VarAC selected. Before preparation it verifies distinct-instance
source intent, built-in Spotter ownership, station-shared CommStat ownership,
and the explicit VarAC arrangement gate. A deterministic bounded-worker result
then proves that only the prepared generation unlocks family review actions.
The same live widget tree retains one body scroll owner, no page-level
horizontal overflow, and a reachable fixed navigation/footer at 1920x1080,
1000x700, and 900x560.

The supplied production configuration database at
`/Users/bill/RadioTools/FIO_DB_prod/current/freqinout.db` was opened only through
SQLite `mode=ro&immutable=1`. The bounded projection confirmed one durably
linked usable JS8Call/Fast Light/VarAC set, seven incomplete diagnostic-only
JS8Call rows, seven incomplete diagnostic-only Fast Light rows, duplicate legacy
endpoint claims normalized once, no manifests, one standalone VarAC node, and no
cluster. The audit did not query traffic, message, ingest, sync, observation, or
operational-history tables. File size and modification time were unchanged;
`freqinout_nets.db` was not opened.

Final primary review corrected one receiver-only integration regression found by
the gate: preparing an SDR no longer requires JS8Call, and observer preparation
continues through selected receive-safe Fast Light and/or distinct JS8Call
companions while excluding FLRig and VarAC transmit/control ownership. The
JS8Call generated configuration value is labeled accurately as a
profile/configuration folder rather than a settings file.

Model ownership and review:

- Primary `gpt-5.6-sol`, high reasoning: final architecture/concurrency,
  delegated-diff review, receiver-only integration correction, transaction and
  fault evidence, partitioned acceptance, production-data safety review, and
  specification/work-log reconciliation.
- `gpt-5.6-luna`, high reasoning: immutable production-database audit,
  production-shaped classification/claim tests, forbidden-table trace check,
  and no-write evidence.
- `gpt-5.6-terra`, high reasoning: exact TriMode real-widget route,
  prepare-publication boundary, fixed navigation/footer, and responsive geometry
  tests.

Acceptance evidence: the GRS-7.5 route plus immutable production audit passes
**8 tests**; the focused GRS-7.2 through GRS-7.4 preparation, topology,
inventory, assistant, recipe, and UI partition passes **103 tests**; the core
proposal/family/recipe/transaction/fault/production-shaped partition passes
**91 tests**; the discovery, performance, save-transaction, manifest, and
Settings-adapter partition passes **65 tests**; and the independently rerun
adjacent family, persistence, Software Administration, launch-recipe,
guided-setup, and performance partition passes **328 tests**. Changed Python
compiles and `git diff --check` passes. No schema, migration, cleanup, production
database, native profile, external process, endpoint, radio, commit, or remote
repository was changed.

The remaining operator-assisted gate must use the current worktree and active
configuration root and must exercise real installed-app discovery plus final
Save, Cancel, and recovery/fault behavior with live JS8Call variants, Fast Light,
VarAC, receiver/radio control, and the required macOS/Linux/Windows platforms.
Until that evidence is recorded, this work is ready for testing but not a
completed release qualification.

### GRS-8 — Zero-Entry Managed Bundle And Thread-Safe Save Remediation

Status: automated remediation gate passed 2026-09-18; operator-assisted Linux
launch/save qualification remains open. The 2026-09-18 Linux operator route
reopened the automated Add Radio and final-Save gate.

Observable reproduction:

`Settings -> Radios -> Add Radio -> Transceiver -> Fast Light + JS8Call + FIO
Spotter + CommStat -> Prepare selected software automatically -> Connections ->
Safety -> Assign schedule later -> Review & Save`

The reported build populated ports and other loose controls, then required the
operator to open a technical software assistant because preparation had not
created the complete in-memory JS8Call and Fast Light bundles required by Save.
The Schedule card also reported `Needed` even though schedule selection was not
the Save predicate. During final Save, a native-configuration worker invoked a
plain Python completion callback on its worker thread; that callback entered the
FIO save transaction, refreshed Qt widgets, and used the GUI-owned
`SettingsManager`. The thread-affinity guard raised and the cross-thread Qt
error path destabilized the application. The transaction reported rollback;
no successful Save claim is made.

#### Field And Action Ownership

For a qualified new managed JS8Call or Fast Light instance, Prepare must publish
one complete immutable draft containing the stable key, exact executable and
variant/version evidence, native/profile/configuration roots, application-data,
message/log/check-in roots, TCP/UDP endpoints, rig/profile selectors, component
launch commands, working directories, dependencies, startup policy, manifest,
resource claims, native-writer state, and recovery guidance. The generated
draft is the same atomic bundle used by Connections, Review, external planning,
persistence, Launch Control, and Health.

Built-in FIO Spotter contributes its station catalog and a binding to the
selected radio-owned JS8 transport. It creates no external application,
launcher, or per-radio forms-folder question. CommStat contributes one binding
from the station-shared CommStat identity to that exact JS8 endpoint; it creates
no duplicate process, executable, profile, or launch item.

The operator normally decides only:

- which software and source intent to use;
- whether FIO launches each eligible managed application;
- role/capability and explicit transmit/safety policy;
- assign a schedule now versus later; and
- whether to accept the readable reviewed plan.

Generated technical values are visible as read-only learning evidence in
Review and `Show details`. They become editable only through an explicit
Advanced override after FIO reports that the qualified recipe cannot complete
the requested result. A missing or ambiguous executable asks the operator to
choose among bounded candidates or Browse; it does not expose unrelated raw
configuration fields.

Preparation creates no directories or files. Final Save is the one commit
boundary. It creates all reviewed FIO-owned directories and qualified native
files, uses temporary targets and backups where required, verifies semantic
readback, saves the complete FIO transaction, and restores exact external state
if the FIO transaction fails. An unsupported native writer does not erase the
safe FIO-owned bundle: Save retains it inactive or verification-pending and
states the one exact operator action.

Schedule is independent from software preparation. `Assign later / no
schedule` is the safe default and a completed state unless the selected FIO
Behavior explicitly requires a plan. A missing optional plan may be a visible
follow-up but may not masquerade as the reason a software draft or radio cannot
be saved.

#### Concurrency And Completion Contract

Workers own bounded discovery, parsing, hashing, backup, external file writes,
and readback. Their success/failure publications cross an explicit queued
QObject boundary into a GUI-affinity receiver. Plain worker-thread callbacks may
not invoke a SettingsTab method that accesses Qt, `SettingsManager`, a
GUI-created store/connection, dialogs, table refresh, or navigation.

The GUI thread owns only cache publication, visible progress, confirmation,
and bounded presentation. Durable database work that can exceed the UI budget
uses a worker-owned store/connection and returns an immutable result; final UI
publication remains queued and generation-fenced. No error or rollback route
may show a `QMessageBox`, refresh a widget, or reuse the GUI-owned
`SettingsManager` from a worker thread.

#### GRS-8 Exit Gate

The slice passes only when all of the following are proven:

1. The exact observable route completes without opening Software
   Administration, Configure Details, a generic Files page, or any raw
   path/port/command/profile editor.
2. Prepare publishes complete qualified JS8Call and Fast Light drafts into the
   radio draft. Review and Save require no manually entered technical value.
3. JS8Call receives distinct profile/application-data/message roots, rig
   selector, TCP/UDP claims, launch identity, and qualified native-plan state.
   Fast Light receives distinct FLRig/FLDigi roots, endpoints, logs/check-in
   paths, component commands, dependencies, and receive-safe defaults.
4. FIO Spotter creates no external instance. Exactly one station CommStat
   identity remains, with one new binding to the prepared JS8 endpoint.
5. `Assign later / no schedule` is the initial safe completed schedule state;
   only a behavior that explicitly requires scheduling blocks Save.
6. Before Save, generated filesystem targets do not exist. After successful
   Save, every reviewed safe directory/file exists, qualified native state reads
   back, the FIO transaction contains no orphan or hybrid identity, and the
   launch plan is executable from its reviewed working directories.
7. Native-worker success and failure callbacks are observed on the GUI thread.
   A focused test fails if a SettingsTab transaction, SettingsManager access,
   Qt refresh, dialog, or navigation runs on a worker thread.
8. Injected discovery, external-write, readback, database, activation,
   schedule, and callback failures leave byte-for-byte external and row-for-row
   FIO prior state, publish one bounded recovery result, and keep the event loop
   responsive.
9. Add Radio and Software Administration share the same prepared-bundle core;
   one path cannot require configuration that the other path derives.
10. The real-widget route passes Normal/Large Text, Light/Dark, 1920x1080,
    1000x700, and 900x560 checks with one body scroll owner, stable navigation,
    reachable primary action, no swipe/vanish reflow, and no page-level
    horizontal overflow.
11. The attached CPU-hotspot evidence is reviewed separately from the crash.
    Repeated scheduler fallback and form-discovery work receives its own bounded
    performance finding/test and is not represented as fixed merely because the
    Save crash is corrected.

Automated success does not close the Linux external gate. The operator must
relaunch the current deployed worktree, confirm its active configuration root,
repeat the exact route with installed applications, and verify launch/readiness
without changing an existing application's identity unexpectedly.

Implementation evidence: Prepare now publishes complete qualified managed
JS8Call and Fast Light drafts without opening the technical assistant; built-in
FIO Spotter and station-shared CommStat do not create external per-radio
instances; a new radio defaults to `Assign later`; stale name, role, backend,
source, management, launch, setup, or software changes invalidate the prepared
context; and final Save rechecks that same context. Existing reviewed JS8Call
variant/version evidence is reused only when the selected executable identity
matches; otherwise qualification remains fail-closed and exposes one bounded
Needs Attention action.

Native guided and VarAC worker success/failure results now cross SettingsTab-
owned queued signals before any callback may access Qt, SettingsManager, or a
GUI-created store. Real event-loop tests reproduce success and failure from a
worker thread and access SettingsManager only after the continuation reaches
the GUI thread. The exact Fast Light + JS8Call + FIO Spotter + CommStat route
now reaches an accepted dialog payload with complete drafts and no schedule or
Plan Builder handoff. The attached sustained-CPU evidence remains a separately
tracked performance gate and is not closed by this remediation.

#### GRS-8.1 Ingest Hotspot Remediation And Remaining Projection Gate

The supplied CPU sample separately captured a message-ingest backfill in which
each status row called the dynamic Spotter form resolver again. That resolver
walks the forms directory, so one backfill could perform the same filesystem
discovery once per record even though the effective mapping cannot change
inside one ingest run. This is not useful freshness; it is repeated work inside
one immutable ingest decision.

One `MessageIngestor` now resolves and caches the effective mapped status-form
IDs once per ingest run. The backfill passes that precomputed set into each
status upsert. A later ingest run receives a new ingestor and therefore a fresh
discovery boundary, preserving changes made between runs while eliminating
same-run directory rescans. Focused tests prove both one-resolution behavior
and that an explicitly precomputed set performs no discovery.

The same trace showed the scheduler's missing-runtime compatibility path
reopening the profile store and rebuilding a radio control client for repeated
lookups of the same radio. That path now has a short per-radio cache fenced by
the endpoint-configuration revision; changed profile/endpoint identities evict
their entry, different radios never share an entry, and negative results expire
quickly. The repeated missing-runtime warning is bounded to one per radio per
30 seconds. This preserves the fail-safe rule: no valid target context still
means no command. Background VarAC status now receives the worker-owned
`SettingsManager` instead of constructing a second settings/schema stack inside
the same poll.

The CPU evidence also records frequent schedule-projection work. That work is
already outside the Qt thread, but its refresh/invalidation policy is a separate
scheduler architecture decision. It must not be changed opportunistically in
the Add Radio crash slice. Before release, a dedicated performance gate must
measure the unchanged-state projection rate, identify the authoritative
database/config invalidation inputs, and prove that a bounded cache cannot hide
assignment, plan, manual-control, or source-backed-plan changes. Until that
gate passes, no claim is made that all sustained CPU causes are resolved.

### GRS-9 — Selected-Family Isolation And Complete Prepared-Bundle Projection

Status: specification corrected and implementation integrated 2026-09-18;
automated acceptance passed, with the installed-Linux operator route remaining
the final live qualification gate.

This section is the controlling contract for the next implementation pass. It
closes defects that the broader prepare-first and zero-entry requirements did
not state with enough observable precision. Where an earlier section permits a
second loose-field, legacy-plan, or assistant-local representation of a guided
software plan, this section supersedes it: one current-generation prepared
bundle map is authoritative from Prepare through final Save. Where an earlier
section blocks Save solely because discovery or version evidence is incomplete,
this section also supersedes it: uncertainty produces a warning and preserves a
safe editable plan; only credible harm blocks the operation.

#### Accuracy, Guidance, And The No-Damage Boundary

FIO aims for the most accurate configuration it can derive, but it does not
confuse confidence with safety. A minor uncertainty in an application version,
optional folder, profile convention, or post-launch readback is not a reason to
prevent the operator from creating a new isolated instance. FIO chooses a
reasonable safe default, identifies the assumption in plain language, allows
the operator to review or change it, and records enough evidence to verify the
result after launch.

Every prepared family has one of four outcomes:

| Outcome | Save behavior | Launch behavior |
| --- | --- | --- |
| `Ready` | enabled | enabled from the reviewed recipe |
| `Ready with warnings` | enabled | enabled when the command and isolated targets are safe; verify after launch |
| `Saved; launch setup pending` | enabled | disabled only until the one missing executable or required command value is supplied |
| `Blocked for safety` | blocked | disabled |

`Blocked for safety` is reserved for a credible risk of damage or an invalid
radio-safety state, including an unapproved overwrite or reuse of existing
configuration, a target/path/database identity collision that FIO cannot make
unique, an unreviewed mutation of shared VarAC cluster data, an endpoint claim
that cannot be made distinct, a transaction that cannot be rolled back, or a
receive-only radio receiving transmit/PTT authority. Missing optional metadata,
an unverified but plausible version, an application that is not running, a
folder the operator can change later, incomplete native readback, or absence of
an existing profile produces a warning rather than a Save block.

Saving the FIO identity and plan is distinct from readiness to launch. If FIO
cannot yet form a safe executable command, it saves the isolated configuration
as `launch setup pending` and names the one remaining action. It does not force
the operator to abandon the radio or re-enter unrelated configuration.

#### Reproduced Defects And Root Causes

The supplied Linux log and screenshots establish four separate defects:

1. VarAC native preparation derives an INI, VARA runtime, launch command,
   ports, and a working root, but publishes those facts only to a technical
   presentation. The actual VarAC instance draft is not hydrated with the
   generated INI, database, incoming, outbox, working-directory, and launch
   fields. Software Administration can therefore display a technically valid
   plan above blank editable fields for the same instance.
2. TriMode selects VarAC. Closing or cancelling the nested VarAC editor does
   not change the parent selection and does not purge its retained draft or
   reservations. A later review can therefore contain VarAC even when the
   operator believes the current plan is Fast Light plus JS8Call only. The log
   for the reported second attempt scanned Applications, Fast Light, and JS8
   profiles but no VarAC phase, confirming that VarAC in Review was UI state,
   not fresh discovery evidence.
3. Fast Light and JS8Call discovery returned zero candidates. JS8 profile
   discovery required 17.319 seconds and still returned zero. Managed-recipe
   qualification then failed, Save remained unavailable, and the visible
   review did not identify the exact missing executable/version evidence as the
   blocking condition.
4. Radios mode leaves a styled compact-header frame visible after hiding all
   of its children, and its content area may consume surplus vertical height
   above the readiness panel. The result is an empty outlined bar and a large
   blank region instead of top-aligned radio content.

The earlier worker-thread `SettingsManager` exception remains governed by
GRS-8. It is a real implementation defect, but the supplied second-attempt log
contains no final Save attempt and must not be misrepresented as the cause of
that attempt's disabled or unreachable Save state.

#### Operator Decisions Versus FIO-Owned Work

The operator decides intent. FIO performs every safe deterministic technical
action. A supported guided route asks the operator only to:

- select the software families assigned to the radio;
- select `Use existing unchanged` or `Create a distinct managed instance` when
  both choices are genuinely available;
- for VarAC, select `Standalone`, `Create cluster`, or `Join cluster`, and
  choose among multiple valid source nodes or clusters only when FIO cannot
  safely select a sole candidate;
- choose whether eligible applications launch with FIO;
- choose explicit email-gateway, cluster PTT-lock, RF/transmit, and other
  safety policy where applicable; and
- accept the final human-readable plan.

For a qualified recipe, FIO owns and derives all of the following without a
workspace, folder, port, profile, command, or configuration question:

- installed executable identity, supported variant, and version evidence;
- stable instance and radio ownership keys;
- collision-free ports and other resource claims;
- managed roots, working directories, profile/configuration files,
  application-data roots, logs, message files, check-in/incoming folders, and
  outbox folders;
- radio/profile/rig selectors and inter-component bindings;
- exact launch commands, arguments, dependencies, and startup order;
- FIO Spotter's built-in binding and the station-shared CommStat binding; and
- qualified native-file creation, directory creation, backup, semantic
  readback, rollback, and recovery at the final Save boundary.

Generated values appear as read-only facts in the normal route so the operator
can learn what FIO will do. `Show technical details` may expose the complete
projection. The operator can choose a meaningful instance name and optionally
Browse for a base folder; FIO appends the stable, sanitized instance name and
derives its configuration, incoming, outgoing, log, and application-data
children. Advanced editing can change an individual derived value without
discarding the rest of the prepared bundle. A sole detected value is selected
automatically; two or more materially different valid candidates produce one
bounded choice. FIO must never ask for a generic `workspace` when the selected
source and stable radio identity determine the target.

FIO records both intended and effective configuration after the fact: the
source and confidence of every discovered or generated value, the exact launch
command and working directory, the paths and endpoints reviewed at Save, the
last successful launch/readiness evidence, and any differences observed after
the application creates or normalizes its own files. A mismatch produces a
guided reconcile choice; it never silently adopts or overwrites another
instance.

#### One Authoritative Draft Session

Each Add Radio dialog owns one non-durable `draft_session_id`, a monotonically
increasing `preparation_generation`, and an authoritative `selected_families`
set. Every prepared family bundle, native presentation, resource reservation,
and discovery result carries all three values plus an intent fingerprint. No
data from another dialog, an earlier generation, a prior preset, or a cancelled
editor may participate in Connections, Review, the Save predicate, or the
commit payload.

Prepare atomically replaces the prepared-bundle map for exactly the current
selected-family set. It does not merge new results into an unbounded retained
map. Deselecting a family immediately and synchronously removes that family's:

- prepared bundle and assistant draft;
- native preparation/presentation and worker publication target;
- endpoint, port, file, cluster-member, and launch reservations;
- Connections and Review rows;
- Save predicates and pending apply actions; and
- final transaction payload.

Changing a preset, source, management policy, launch policy, radio name/role,
or VarAC arrangement increments the generation and invalidates every dependent
bundle. TriMode explicitly selects Fast Light, JS8Call, and VarAC. Removing
VarAC changes the setup summary to a custom Fast Light + JS8Call selection and
must remove VarAC everywhere immediately.

The nested editor must not use an ambiguous `Cancel` label. Its two meanings
are separate actions:

- `Back without changes` discards edits made in that editor but keeps the
  family selected and returns to the Software step, where any unmet requirement
  remains plainly visible; and
- `Remove <family> from this radio` deselects the family and performs the full
  purge above.

Cancelling the outer Add Radio dialog destroys the complete draft session,
cancels or generation-fences every worker result, releases all reservations,
and compensates any preview/native operation. Opening Add Radio again starts
with a new empty session. It may read durable station inventory but may not
reuse any selection, prepared draft, presentation, or reservation from the
cancelled session.

Connections, family cards, Review, Save validation, and persistence all read
the same immutable current-generation prepared-bundle map. Review may not
reconstruct a second plan from checkboxes, loose form fields, preset defaults,
or the legacy external-application planner. An unprepared selected family is a
single `Needs attention` item with one direct recovery action; it is never
shown as though it has a prepared native action.

#### Canonical VarAC Bundle

An existing VarAC installation is source evidence, not a workspace. FIO first
discovers the executable, its supported version, native configuration, and the
existing node's INI, database, incoming, outbox, working directory, VARA
runtime, and effective launch identity. `Use existing unchanged` presents
those discovered values as source-locked read-only facts. It asks the operator
only to resolve a genuine ambiguity or one specifically missing value.

`Create a distinct managed instance` reuses only the qualified application
installation and immutable source evidence. FIO creates a new node identity
and derives node-local targets below the stable managed-instance root. It never
reuses another node's INI, incoming, outbox, working directory, ports, or launch
identity. For a first cluster created from an existing standalone node, the
reviewed standalone database becomes the cluster-shared database; the new
member receives its own managed INI, VARA runtime, incoming folder, outbox
folder, working directory, ports, and launch command. Joining an existing
cluster uses that cluster's reviewed shared database and still creates distinct
node-local resources. Standalone never silently becomes cluster mode.

The canonical prepared VarAC bundle contains at least:

| Prepared fact | Canonical draft field |
| --- | --- |
| qualified VarAC installation/launcher | `application_path` |
| effective or generated VarAC INI | `configuration_path` |
| node-local or cluster-shared VarAC database | `storage_path` |
| distinct incoming folder | `secondary_storage_path` |
| distinct outbox folder | `outbox_path` |
| exact node working directory | `working_directory` |
| VARA runtime and VARA INI | native component records |
| exact VarAC/VARA commands and order | launch recipe/component records |
| command, KISS, and VARA ports | resource claims/endpoints |
| cluster ID, member number, gateway and PTT policy | topology/policy records |

Native preparation, the family card, Software Administration, Connections,
Review, persistence, and Launch Control must render this same bundle; no view
may maintain a partial translation. Publication of a qualified native result
must hydrate the bundle before the Files or Review page can render. A generated
technical summary above blank editable fields is a failing state.

For an exact qualified native writer, the normal route has no required editable
VarAC path fields and the operator does not need to enter incoming, outbox,
database, INI, or working-directory values. The normal path editor presents one
optional base folder plus the meaningful instance name and previews the derived
children. If the installed VarAC version or source cannot be qualified, FIO
preserves all safely derived draft facts and distinguishes two cases: a new
isolated node with no existing-data mutation is saved with a warning or with
launch setup pending; a requested shared-database or cluster mutation that
cannot be written and rolled back safely is blocked for safety. Neither case
falls back to a generic empty Files form.

#### Fast Light And JS8Call Qualification

Creating a distinct Fast Light or JS8Call instance reuses a qualified
application executable, not an existing instance profile. Searching existing
profiles is optional evidence and may not delay or block creation of a new
managed profile. FIO checks, in order, reviewed durable inventory, saved exact
executable identities, platform application/package metadata, and bounded
well-known/PATH candidates such as `/usr/bin`. Version evidence comes from a
reviewed saved identity or a qualified non-interactive app-specific metadata
probe with a hard timeout; FIO must not require a version token to appear in
the executable path and must not launch an unqualified GUI binary merely to ask
for its version.

When a plausible executable is found, FIO generates the complete distinct
profile and launch bundle even when no existing profile candidate exists or
exact version evidence is unavailable. JS8Call, FLRig, and FLDigi receive
unique profile/configuration roots at launch; those roots, their arguments, and
the exact effective command are stored by FIO for later launch, readiness,
audit, and reconciliation. Fast Light and JS8Call review must consume those
retained bundles, not a generic native-profile action reconstructed from loose
fields.

Qualification confidence changes the status, not the isolation contract. An
unverified version produces `Ready with warnings` when the executable and
distinct launch arguments are otherwise safe. A missing executable produces
`Saved; launch setup pending` and a Browse action. The family card and Review
state the precise concern, for example `JS8Call Subspace was found at
/usr/bin/js8call-subspace; FIO prepared a distinct profile, but the exact
version will be verified after first launch`. A disabled button or `0
candidates` telemetry is not an operator-facing explanation.

After first launch, FIO performs bounded readiness and filesystem reconciliation
against the saved intent. If the application created or normalized a different
profile location, FIO reports the difference and offers `Use the observed
location` or `Keep the reviewed location and correct launch`. It does not
silently relink the instance and does not touch an existing profile.

#### Responsiveness, Telemetry, And Layout

Prepare returns event-loop control and shows per-family progress within 100 ms.
All filesystem, package, profile, and version discovery runs on bounded workers.
Durable inventory and well-known executable lookup are the fast path. Existing-
profile discovery is lazy for create-new routes and may not hold the result
until a recursive scan finishes. Each discovery phase has a two-second soft
budget, publishes useful partial results when available, and offers a bounded
recovery action rather than continuing an unbounded search. The 12-17 second
zero-result JS8 scan is an acceptance failure even though it runs off the GUI
thread.

Telemetry for each draft session records preset changes, family selection and
removal, preparation generation, editor outcome, discovered executable and
version-evidence source, bounded phase duration, current Save-enabled state,
and exact blocker codes. Paths containing operator data use the existing
redaction policy. Logs must make it possible to distinguish `Back without
changes`, `Remove family`, outer-dialog Cancel, Review, and Save without
inferring clicks from screenshots.

In Radios mode, a header or frame with no visible semantic child collapses to
zero height and draws no border. Radio Profile content is top-aligned directly
below its title; expanding containers or spacers may not insert an elastic
blank region before the readiness dashboard. The normal and Large Text layouts
retain one body scroll owner and a reachable action/footer area.

#### GRS-9 Exit Gate

The next implementation slice passes only when all of the following are proven
against the final integrated tree:

1. With an isolated copy of the production database, selecting TriMode and
   preparing a supported first VarAC cluster from the sole existing standalone
   node yields nonblank read-only application, INI, database, incoming, outbox,
   working-directory, VARA, port, and command facts. No workspace or raw-path
   question appears and final Save is reachable.
2. A qualified `Use existing unchanged` VarAC route discovers and displays all
   effective source-locked facts without creating files. A managed create/join
   route derives distinct node-local facts and the correct shared database.
3. `Back without changes` retains a selected VarAC family and shows its exact
   status. `Remove VarAC from this radio` immediately removes every VarAC card,
   connection, review line, native action, reservation, Save predicate, and
   payload value.
4. After starting TriMode, removing VarAC, and preparing Fast Light + JS8Call,
   Review and the accepted dialog payload contain no VarAC text, field, action,
   cluster record, or filesystem target. Cancelling the entire first Add Radio
   attempt and opening a second Fast Light + JS8Call attempt has the same clean
   result.
5. On Linux, plausible Fast Light and JS8Call executables in `/usr/bin` prepare
   complete new managed profiles without reusing an existing profile and
   without opening Software Administration. Exact ports, files, commands, and
   dependencies appear in the canonical review bundle and Save is enabled.
   Missing exact version evidence produces `Ready with warnings`, not a block.
6. Zero existing JS8 profile candidates is a valid create-new condition. A
   missing executable, ambiguous executable, unverified version, unsupported
   writer, or collision each produces a different visible concern and one
   bounded recovery action. Only an unresolved destructive collision,
   unapproved existing-data mutation, unsafe shared-cluster change, invalid RF
   authority, or non-rollbackable transaction blocks Save.
7. Prepare remains interactive, progress appears within 100 ms, no discovery
   phase exceeds its bounded contract, and a recursive profile scan cannot
   delay a new-profile plan by 12-17 seconds. Generation-fenced late results
   cannot repopulate a removed family.
8. Final Save exercises the accepted dialog payload, creates every reviewed
   FIO-owned directory/file, verifies native readback, and persists exactly the
   current selected families. Injected failure and outer Cancel preserve the
   prior database and filesystem byte-for-byte and leave no reservation.
9. Real-widget tests prove that family cards, Connections, Review, Save
   validation, native apply, persistence, and Launch Control all receive the
   same canonical bundle identity and fingerprint; no parallel legacy plan is
   invoked for a guided selected family.
10. Normal/Large Text at 1920x1080, 1280x720, 1000x700, and 900x560 shows no
    empty outlined compact header, no elastic blank region above Radio Setup,
    no page-level horizontal overflow, and reachable primary actions.
11. The exact user-visible route is run once with supported installed Linux
    applications after automated tests pass. The result records selected
    families, detected versions, generated roots and ports, launch/readiness,
    and confirms that no existing profile or production database was changed
    unexpectedly.
12. Warning-policy tests prove that unverified version evidence, missing
    optional folders, absent existing profiles, and incomplete post-launch
    readback permit Save with the correct status. Safety-policy tests prove that
    overwrite, shared-database mutation without a qualified transaction,
    unresolvable identity/resource collision, non-rollbackable apply, and
    receive-only transmit authority remain blocked.

The automated gate must inspect rendered fields, prepared-bundle contents,
accepted transaction payload, created files, persisted identities, and launch
plan. Tests that assert only button existence, button enabled state, or helper
return values do not satisfy this gate.

#### GRS-9 Implementation Evidence — 2026-09-18

The integrated implementation now enforces the no-damage boundary and the
single prepared-plan projection described above:

- managed JS8Call and Fast Light recipes preserve generated profile roots,
  endpoints, working directories, commands, confidence, and discovery evidence
  for `Ready`, `Ready with warnings`, and `Saved; launch setup pending` states;
  launch-pending bundles persist with automatic launch disabled;
- stock JS8Call, JS8Call Subspace, and JS8Call Improved executable names select
  the reviewed launch-argument family without inventing a version. A plausible
  installed executable with no exact version is warning-ready; an arbitrary
  Browse target remains launch-pending;
- only explicit overwrite/reuse, identity/path/endpoint/resource collision,
  missing distinct identity or endpoints, unsafe shared mutation, transaction,
  or RF-authority conditions use `Blocked for safety` and disable Save;
- VarAC native preparation publishes application, INI, database, incoming,
  outbox, working-directory, VARA runtime/INI, ports, and launch command into
  the authoritative assistant draft before Files or Review renders;
- deselecting a family synchronously purges its retained assistant/native state,
  while the nested editor exposes distinct `Back without changes` and
  `Remove <family> from this radio` actions;
- selected-family Review text and Save validation consume the retained recipe
  state, so removed VarAC content cannot survive into a Fast Light + JS8Call
  payload;
- executable discovery prioritizes saved exact identities, keeps well-known
  lookup bounded, applies a two-second per-phase budget, publishes safe partial
  evidence, and generation-fences late results; and
- the empty Radios header collapses and Radio Profile content uses a top-aligned,
  content-sized layout.

The final-tree automated matrix passed **396 tests** across recipe resolution,
warning/safety policy, discovery budgets, real-widget guided routes, VarAC
native preparation and transactions, manifest/store persistence, launch
identity, receiver scope, and layout behavior. Python compilation and
`git diff --check` passed. Three production-shaped checks used immutable reads
and a disposable copy of
`/Users/bill/RadioTools/FIO_DB_prod/current/freqinout.db`; source size,
timestamp, and hash were unchanged. No production database or native
application profile was written. The remaining GRS-9 item is the explicit
operator run with the supported applications installed on Linux; it is not
represented as completed by automated macOS tests.

### GRS-10 — Canonical Software Bundle And Platform Launch Contract

Status: specification accepted and automated implementation gate passed
2026-09-18; live Windows and Linux/Wine qualification remains open. This
section supersedes any earlier text that permits assistant-local state, a
flattened command string, an eager native apply, or a second review plan for a
guided software family.

#### Binding Operator Evidence And Root Cause

On the exact route `Settings > Radios > Add Radio`, operator testing showed a
prepared VarAC plan in Software Administration while the parent Add Radio
Connections page retained blank VarAC fields. The nested assistant could not
complete `Save as draft` because it treated a deliberately shared cluster
database as a duplicate private-storage collision. Until that completion, the
parent copied none of the prepared fields. Review then reconstructed VarAC from
loose fields and displayed the installation directory as an effective command.
Separately, persistence flattened a valid argument vector into text and launch
reparsed it with POSIX shell rules, which can remove backslashes from a Wine
Windows path. These are code defects, not operator-entry omissions.

The correction applies to every supported service. It is not sufficient for a
technical preview to contain a value while Connections, Review, persistence,
or Launch Control uses another representation.

#### One Canonical Prepared Software Bundle

Each selected software family owns one immutable, generation-fenced
`PreparedSoftwareBundle`. The bundle is authoritative from automatic
preparation through draft editing, Connections, Review, final transaction,
persistence, launch, readiness, and later reconciliation. It contains:

- stable draft-session, generation, radio, family, instance, and component
  identities;
- platform and source evidence, executable files, supported variant/version,
  and confidence;
- configuration, data, message, log, incoming, outbox, database, and working-
  directory paths, with ownership (`instance`, `cluster`, `station`, or
  `built-in`) and mutation policy;
- endpoints and collision-free resource claims;
- for every external component, a structured argument vector, working
  directory, environment overrides, dependency order, launch policy, and
  readiness policy;
- the exact staged filesystem/native mutations, backup/readback/rollback plan,
  warnings, safety blockers, and plan fingerprint; and
- intended and subsequently observed/effective state for audit and guided
  reconciliation.

An argument vector is the durable launch authority. A shell-formatted string is
display-only and must never be reparsed to recover the command. No guided view
may rebuild this bundle from checkboxes, loose widgets, an installation path,
or the legacy external-application plan. Publication replaces the current
family bundle atomically and immediately makes the same bundle available to
the parent Add Radio flow; entering or completing every nested page is not a
prerequisite for parent projection.

#### Service Responsibility Matrix

| Family | FIO safely prepares and owns | Operator decisions | Never inferred or duplicated |
| --- | --- | --- | --- |
| JS8Call family | qualified stock/Improved/Subspace executable, unique radio data/profile root, settings/message paths, unique API port, radio binding, structured launch recipe, readiness and reconciliation | family variant only when more than one qualified choice exists; launch with FIO | another instance's profile, API port, or data root |
| Fast Light | distinct FLRig and FLDigi profiles, ports, bindings, launch order and structured recipes; FLMsg/FLAmp executable and dependency recipes when selected | launch policy and explicit advanced transmit behavior for a transceiver | another radio's profiles or endpoints; transmit authority for an observer |
| FIO Spotter | built-in radio binding and known internal resources | enable/disable | external process, launcher, or MCF path question in the normal route |
| CommStat | one station-shared service plus a per-radio JS8 endpoint binding and health evidence | enable/disable binding | a second radio-owned CommStat process or duplicate station configuration |
| VarAC/VARA | qualified installation, topology, unique member identity, ports, INI, VARA runtime/configuration, incoming/outbox, working directory, structured commands, dependencies, native transaction and recovery | standalone/create/join, genuine source ambiguity, email gateway sender, PTT lock, launch policy | silent standalone conversion, another member's INI/runtime/ports, or an unreviewed shared mutation |

This matrix is the minimum cross-service contract. A supported recipe may add
facts, but it may not shift a deterministic safe configuration task back to the
operator. A missing optional fact is a warning. Only the GRS-9 no-damage
boundary may block final Save.

#### VarAC Layout And Platform Contract

The qualified VarAC installation is the native configuration home. Consistent
with VarAC's cluster guide, FIO uses one installation and a distinct INI name
per instance in that installation, plus a distinct VARA runtime and unique
ports per member. For radio `FT-710`, a typical qualified Linux/Wine plan is:

- host executable: `/home/bill/.wine/drive_c/VarAC/VarAC.exe`;
- host INI: `/home/bill/.wine/drive_c/VarAC/VarAC-FT-710.ini`;
- Windows INI argument: `C:\\VarAC\\VarAC-FT-710.ini`;
- structured arguments: `["wine", "/home/bill/.wine/drive_c/VarAC/VarAC.exe",
  "C:\\VarAC\\VarAC-FT-710.ini"]`; and
- working directory: `/home/bill/.wine/drive_c/VarAC`, with the discovered
  `WINEPREFIX` recorded when it is not the platform default.

The corresponding FIO-created VARA runtime must also remain inside the
discovered Wine drive—for example host folder
`/home/bill/.wine/drive_c/VARA-ft-710` and native VarAC value
`C:\VARA-ft-710\VARA.exe`. FIO must not generate
`Z:\home\...\.freqinout\managed-instances\...\VARA.exe` for this field.
Each converted or new cluster member receives its own readable, space-free
runtime sibling; occupied targets are preserved and resolved through the
existing numbered-sibling allocator.

On Windows, the vector is `[C:\\VarAC\\VarAC.exe,
C:\\VarAC\\VarAC-FT-710.ini]` and the working directory is the installation
directory. Linux/Wine and native Windows are release platforms. macOS remains
best-effort and may report `Saved; launch setup pending` rather than invent a
Wine contract.

FIO may use a different sanitized INI filename selected by its stable identity,
but it must be install-adjacent when the qualified installation supports the
native writer. It must not silently substitute a `Z:` path below
`~/.freqinout` for that INI. If the installation is not writable or its Windows
mapping cannot be proven, FIO preserves the prepared bundle, explains the one
concern, and offers a reviewed location choice or launch-pending state without
changing an existing installation.

The VarAC database is private for standalone instances and cluster-owned for a
cluster. Sharing the reviewed cluster database among members is intentional and
must not trigger the generic duplicate-private-storage blocker. The collision
remains blocking if the same path is claimed as private storage, belongs to a
different cluster, or would be overwritten without the qualified transaction.
INI files, incoming/outbox folders, working identities, VARA runtimes, and ports
remain member-distinct.

Persisted VarAC clusters and memberships are authoritative over the optional
empty-editor display preference. Any saved topology forces cluster setup
visible and populated in Settings/Software Administration, even if the prior
`varac_cluster_mode_enabled` value is false. The control remains on until the
saved topology is removed; a stale preference may not make a completed Add
Radio cluster appear absent.

#### Draft, Final Save, And Recovery Boundary

Automatic preparation and `Save as draft` are non-mutating. They may inspect
reviewed evidence, reserve in-memory resources, and construct staged bytes, but
they do not create application directories/files, edit native configuration,
or persist a radio. `Save as draft` validates internal consistency, returns the
complete bundle to Add Radio, and remains available for `Ready`, `Ready with
warnings`, and `Saved; launch setup pending`. It does not require an external
application to be running and it does not reject an intentional cluster-shared
database.

Only final `Save Radio and Software`, after the human-readable review, may run
the accepted transaction:

1. revalidate the bundle fingerprint, current source digests, process/stop
   requirements, resource claims, platform mapping, and RF authority;
2. create staged instance-owned directories/files and qualified native files;
3. back up any reviewed existing target, atomically promote, and semantically
   read back the exact allowlist;
4. persist the identical bundle, structured launches, radio assignments, and
   recovery journal in the FIO database; and
5. finalize only after both external and database commits succeed.

Cancel, Back, stale generation, injected failure, or FIO database commit
failure restores the prior external bytes and database state, releases draft
reservations, and leaves existing configurations untouched. Recovery is
idempotent and launch remains blocked only for the affected unresolved journal.

#### Required Operator Presentation

The primary task is `Review and save the software FIO prepared for <radio>`.
Automatic discovery/preparation appears before technical administration. The
normal route shows family, status, purpose, significant generated identity,
ports, launch policy, warnings, and one recovery action. Derived paths and
arguments are read-only facts. `Show technical details` reveals the complete
bundle, including exact structured command display, working directory,
environment, files, ownership, evidence, and mutation plan. Advanced editing
is a correction path, not a required form.

Software Administration uses the same bundle and the action `Save as draft`;
its successful result returns to Add Radio without external changes. The final
primary action is `Save Radio and Software`. Every failure or warning states
what FIO determined, what FIO will do, and the one operator action if any. The
dialog uses one body scroll owner, a persistent reachable footer, the shared
theme/type scale, and no raw technical dump by default.

#### GRS-10 Exit Gate

Implementation passes only when all of the following succeed against the final
integrated tree:

1. The reported first-cluster route prepares VarAC, shows the complete bundle
   in both Software Administration and Add Radio before nested completion, and
   enables non-mutating `Save as draft`; the intentional shared database is not
   diagnosed as duplicate private storage.
2. Windows and Linux/Wine fixtures produce install-adjacent unique VarAC INIs,
   platform-native structured argument vectors, correct working directories
   and Wine-prefix environment, distinct VARA runtimes/ports, and a reviewed
   cluster-shared database. macOS reports its bounded support state.
3. Argument vectors containing spaces, backslashes, and Windows drive paths
   round-trip through the store and launch orchestration byte-for-byte without
   shell reparsing. Review displays the executable file and arguments, never an
   installation directory as the effective command.
4. Parent Connections, family cards, Software Administration, Review, final
   transaction, persistence, and Launch Control assert the same family bundle
   ID, generation, fingerprint, paths, endpoints, and launch components.
5. Final Save creates every reviewed FIO-owned directory/file exactly once and
   persists exactly the selected families. Cancel, draft save, stale result,
   and injected failure leave the source database and every existing
   application file byte-for-byte unchanged.
6. Focused end-to-end fixtures cover JS8Call stock/Improved/Subspace, FLRig,
   FLDigi, FLMsg, FLAmp, FIO Spotter, station-shared CommStat, and VarAC/VARA in
   one-instance and multi-instance plans. Each proves the matrix above,
   structured launch persistence, dependency order, unique resources, and no
   cross-radio profile reuse.
7. A copied production-shaped database opens, prepares, saves, reloads, and
   relaunches the canonical bundles through any additive migration. The source
   database hash, size, and timestamp remain unchanged.
8. Real-widget tests exercise the exact route at Normal/Large Text and the
   supported viewport matrix. Preparation remains asynchronous and bounded;
   no page clips the draft/final action or performs synchronous discovery.
9. The governing specifications and work log record each work package, exact
   model/reasoning, commands, counts, skips, human-review limits, and the still-
   open Windows/Linux live qualification gate.

Helper-only and button-enabled tests do not satisfy this gate. The acceptance
suite must inspect rendered values, accepted payloads, staged/committed files,
durable bundles, and the launch request received by the process runner.

#### GRS-10 Implementation Evidence — 2026-09-18

The final integrated implementation uses one reviewed VarAC native plan from
automatic preparation through parent Add Radio projection, non-mutating draft
save, final native apply, FIO transaction, launch-bundle persistence, planner,
and process start. Qualified Windows/Linux-Wine plans use an install-adjacent
unique INI, distinct managed VARA runtime and ports, reviewed cluster-owned
database, executable path, structured argument vector, working directory, and
Wine-prefix environment. The nested editor no longer invokes the writer;
`Save as draft` returns the complete plan, and only accepted outer `Save Radio
and Software` starts the rollback-capable external/FIO transaction.

Add Radio now prepares and projects VarAC without requiring the operator to
open every nested details page. Connections and Review show the VarAC INI,
database, incoming/outbox, VARA runtime/INI, ports, executable, arguments, and
working directory from the retained bundle. Qualified VarAC is excluded from
the obsolete generic read/import-only presenter. Intentional same-cluster
database sharing is valid while private-path, cross-cluster, endpoint, INI,
runtime, and overwrite collisions remain safety blockers.

Structured VarAC launch data is persisted through the existing additive
launch-bundle readiness JSON seam. Store reload, station planning, and launch
orchestration preserve spaces, backslashes, drive letters, arguments, cwd, and
environment without shell reparsing. The process-runner acceptance test
observed the exact vector passed to `subprocess` with `shell=False`. Legacy
VarAC command text remains a compatibility fallback only when no structured
recipe exists. Existing JS8Call and Fast Light structured recipes, built-in
FIO Spotter, and station-shared CommStat behavior remain covered by the
integrated matrix.

Final-tree evidence is **508 unique passing tests** in bounded partitions,
including native writer/rollback, exact Add Radio widgets, draft/final
transaction handoff, cross-service guided recipes, manifest/store reload,
Windows and Linux/Wine launch vectors, responsive/layout contracts, and an
immutable production-shaped source plus migrated disposable copy. Changed
Python compilation and `git diff --check` passed. The source production
database retained its size, timestamp, and SHA-256 hash. No production native
profile, process, endpoint, radio, or remote was changed.

The remaining release gate is an operator walkthrough on supported Windows
and Linux/Wine installations. Automated macOS/offscreen fixtures do not claim
that external VarAC/VARA binaries launched or communicated successfully.

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
- `Create a distinct instance` reuses only a qualified executable/recipe by
  default; it never silently reuses another instance's profile, data, endpoint,
  or manifest.
- CommStat is station-shared with explicit per-JS8 bindings unless a future
  version-qualified contract requires separate processes.
- VarAC arrangement is selected before details. A fresh station defaults to
  standalone; one or more clusters never cause auto-join; and a station with
  standalone node(s) but no cluster shows create-cluster-with-existing as a
  recommendation that requires explicit selection. Cluster mutation is always
  opt-in and final-review confirmed.
- FIO Spotter's normal MCF catalog is resolved by FIO and is not a radio-scoped
  external launch or folder-selection task.

## 2026-09-19 — GRS-10 Cluster-Shared BBS Projection Correction

Status: automated implementation gate passed 2026-09-19; Windows and
Linux/Wine operator qualification remains open.

The canonical prepared VarAC bundle includes cluster-shared `bbs_path` and
`bbs_archive_path`. Native-managed cluster persistence stores them as
`varac_clusters.shared_bbs_path` and
`varac_clusters.shared_bbs_archive_path`. A create-cluster route inherits
nonblank reviewed standalone-profile values; any missing value is derived from
the qualified VarAC installation as `<VarAC install>/BBS` and
`<VarAC install>/BBS/Archive`. A join-cluster route inherits the selected
cluster values. Incoming and outbox remain member-local and may not be
reclassified as shared BBS resources.

The parent Add Radio Connections and Review surfaces, plus Software
Administration, project these canonical facts as prepared read-only values.
They do not invent member persistence or expose a second editable ownership
path. A missing core publisher field may retain an existing value only as a
safe display fallback while the canonical bundle is upgraded.

Only accepted `Save Radio and Software` may create reviewed missing BBS/archive
directories. It never replaces, removes, or clears an existing directory or
its contents. A successful external apply records its FIO-created directory
set; same-session compensation removes only those directories and only while
they remain empty. Recovery never guesses that an unrecorded directory is safe
to delete.

Native readback enrichment is expected output of the accepted plan. Observed
path/digest/verification additions must not invalidate the operator's Review.
The final mutation boundary instead compares current durable cluster/member
inventory with the original frozen reviewed payload; a material inventory
difference is stale, while readback-only enrichment is not. Journal terminal
states `complete`, `fio_committed`, and `rolled_back` are idempotent: repeated
cleanup never restores a committed apply and never restores an already rolled
back apply. `recovery_required` is an unfinished blocking state; automatic
startup recovery never rolls forward and leaves unresolved compensation for
explicit operator recovery.

Work-package ownership: primary `gpt-5.6-sol`, high reasoning, owns schema,
migrations, transaction, concurrency, integration, and final gate;
`gpt-5.6-terra`, medium reasoning, owns bounded UI projection, focused UI
tests, and documentation; `gpt-5.6-luna`, medium reasoning, owns focused
regressions. Automated final-tree evidence: **139 passed**, changed Python
compilation passed, production-copy migration/reload and integrity checks
passed, and diff hygiene passed. The live Windows/Linux-Wine VarAC/VARA
operator walkthrough remains an external release qualification and is not
claimed as passed.

## GRS-11 — Automatic Preparation And Decision-First Software Cards

### Operator Outcome

The Add Radio Software step is one continuous decision flow, not a sequence of
checkbox selection, scrolling, a separate Prepare command, and a second pass
through the same software. The operator selects each software family exactly
once. FIO immediately starts the safe discovery and preparation work that can
be derived from those selections. The operator is asked only for a decision
that FIO cannot safely infer.

The normal route is:

`Select software -> FIO prepares in the background -> resolve any highlighted
choice -> review compact Ready cards -> Next: Connections`.

There is no required global Prepare button in this route. A retry action is
shown only after preparation fails or becomes stale. The selected checkboxes
remain the sole family-selection source and the accepted radio payload must
match them exactly; family cards may explain or correct a selected family's
plan but never duplicate its selection state.

### Automatic Preparation State Machine

Any change to a selected family, source choice, management choice, launch
policy, or explicit VarAC arrangement invalidates the prior prepared context
and schedules a new preparation pass after a short coalescing interval. Rapid
checkbox changes produce one pass for the final state. Preparation reports
visible progress without blocking the GUI and without moving the operator to
another page.

The existing generation-fenced background discovery path remains the only
preparation path. At most one pass is authoritative at a time. If inputs change
while a pass is running, FIO cancels or disregards that generation, preserves
the new selections, and starts one coalesced replacement pass. A result may be
published only when its captured software-plan context still equals the live
context. Dialog close, cancellation, and superseded generations may not publish
drafts or mutate external software.

VarAC topology is an explicit exception to inference. When VarAC is selected,
FIO first presents the decision between standalone, create cluster, or join
cluster as applicable. It does not start VarAC preparation until that decision
is made and never infers cluster membership. Once selected, the same automatic
preparation state machine applies.

Automatic preparation is preview-only. It may inspect software and prepare
canonical drafts; it does not create profiles, write native configuration,
launch processes, or change durable FIO state before accepted final Save.

### Compact Per-Family Cards

The Software step shows one card for each selected family and no card for an
unselected family. Each card has a single state:

- **Preparing** — compact progress; no technical form.
- **Ready** — collapsed summary with source, launch policy, significant
  identity or endpoints, and optional `Details`.
- **Ready with warning** — collapsed summary plus the specific non-blocking
  warning and optional `Details`; the operator may continue.
- **Launch pending** — the saved plan is valid but the application is not yet
  running or verified; the operator may continue.
- **Needs choice** — only the unresolved operator decision and its explanation
  are expanded inline.
- **Blocked** — the safety condition and one recovery action are expanded; the
  operator may not continue until it is resolved or the family is deselected.

Ready families do not expose source, completion, management, launch-policy, or
path forms in the normal route. `Details` opens the existing family editor for
inspection or intentional correction. Opening and cancelling Details is
non-mutating. Qualified FIO-derived paths, arguments, commands, and files remain
read-only facts; unsupported or ambiguous recovery fields appear only in the
advanced correction route.

After preparation, FIO may bring the first unresolved card into view once for
that result generation. It must not repeatedly steal scrolling after the
operator moves the page. Ready cards stay collapsed so they do not displace the
decision that requires attention.

### Navigation And Severity

The existing Back/Next/Cancel/Save footer remains outside the single body
scroll area and reachable at every supported viewport and text scale. On the
Software step:

- `Next: Connections` is disabled while preparation is active, while required
  preparation is stale, or while a selected family has a safety blocker or an
  unresolved required choice.
- `Next: Connections` is enabled for Ready, Ready with warning, and Launch
  pending states. Warnings explain the consequence and remain visible in Review
  but do not force a configuration loop.
- Invoking Next against stale inputs schedules preparation and keeps the
  operator on Software. It does not silently accept an older result.
- The first unresolved card receives focus/visibility once; the global page is
  not scrolled back to a former Prepare control.

### Performance And Accessibility Contract

Selection feedback and a Preparing state appear within 100 ms. Changes are
coalesced before discovery, and no duplicate worker is started for an identical
live context. All preparation remains off the GUI thread. Card status, warning,
and actions have stable object names and accessible text; state is conveyed by
text in addition to color. Large Text, keyboard navigation, and the supported
viewport matrix retain a single vertical body scroll and the fixed footer.

### GRS-11 Exit Gate

Implementation passes only when all of the following succeed against the final
integrated tree:

1. Selecting JS8Call, Fast Light, FIO Spotter, CommStat, or VarAC starts the
   existing safe preparation path without clicking or scrolling to a Prepare
   button; a checkbox burst produces one authoritative pass.
2. A mid-flight selection/source change cannot publish the stale generation;
   the replacement result preserves the final visible selections and its
   accepted payload and retained draft keys match those selections exactly.
3. VarAC waits for explicit topology and then prepares automatically. No
   standalone/create/join choice is inferred.
4. Ready cards collapse; only the first unresolved family expands and is made
   visible once. Details may be opened and cancelled without changing the
   parent selection, canonical draft, or preparation count.
5. Warning and launch-pending families allow Next. Preparing, stale,
   needs-choice, and safety-blocked families prevent Next and show one clear
   recovery action.
6. Scrolling the body to either extreme leaves Back and Next reachable and
   functional. Normal and Large Text layouts do not introduce a second page
   scrollbar or hide the footer.
7. Existing canonical bundle, native writer, transaction, rollback, selected-
   family isolation, and production-copy acceptance suites remain green. No
   schema or migration is introduced by this UI slice.

### GRS-11 Implementation Evidence — 2026-09-19

The Software step now treats preparation as an automatic state transition.
Family, source, completion, management, launch, radio identity, and explicit
VarAC-arrangement changes invalidate one canonical context and enter a 200 ms
coalescing window. The existing background coordinator remains authoritative.
An in-flight change cancels or rejects the old generation, and its result is
published only when the captured context still matches the live dialog. The
normal Prepare button is hidden; it returns only as `Retry preparation` after
a recoverable failure.

Selected-family cards now present explicit textual states. Preparing cards hide
forms. Ready, warning, and launch-pending cards collapse to a concise status,
source/launch/endpoint facts, and optional Details. Needs-choice and blocked
cards expose the required decision or recovery action and are marked expanded.
The first unresolved card is brought into view once per prepared context. The
existing Details editors remain the intentional correction route and cancelling
them does not change the parent selection, drafts, or preparation count.

The Software Next action uses the same managed-recipe, native-VarAC,
detected-choice, and no-damage tests as final Save. Preparing, stale, missing
VarAC topology, missing managed drafts, and explicit safety blocks stop Next;
ready-with-warning and launch-pending plans continue. The action footer remains
outside the one body scroll owner.

Work-package ownership: primary `gpt-5.6-sol`, high reasoning, owned the state
and concurrency contract, specification, delegated-diff review, integration
corrections, and final gate; `gpt-5.6-terra`, medium reasoning, owned the
bounded Settings UI implementation and its initial focused verification;
`gpt-5.6-luna`, medium reasoning, owned the focused test audit and real-widget
regressions.

Final-tree evidence is **215 unique passing tests** in non-overlapping
partitions: 82 guided Add Radio/VarAC/native transaction tests, 118 canonical
inventory/recipe/persistence/launch-bundle tests, and 15 production-shaped
inventory/launch round-trip tests. The focused GRS-11 UI subset is 35 passing
tests. Changed Python compilation and `git diff --check` pass. This slice adds
no schema or migration and performs no production database, application-file,
process, endpoint, radio, commit, or remote mutation.

### GRS-11.1 — Detected Application Choice And Launch-Details Stability

An explicit detected-application selection is authoritative. If the operator
chooses a JS8Call, Fast Light, or other detected executable, FIO replaces any
provisional path for that family, invalidates the old launch recipe, and
automatically prepares exactly one replacement plan. Next remains disabled
while that replacement is pending and is republished immediately when the
choice resolves the last ambiguity. A choice matching the current path does
not start another worker but still refreshes navigation state.

Automatic selection of the sole detected candidate occurs inside the current
preparation result and must not recursively start a second worker. A manual
Browse choice follows the same authoritative replacement and preparation
contract as a detected choice.

`Launch pending` is a saveable warning, not a required administration detour.
Its family action is labelled `Review Details (optional)` and the operator may
continue. When Details is opened, its one-scroll assistant and fixed footer
must fit inside the current screen's available geometry with a desktop margin;
compact Linux window managers may not move an oversized nested modal off-screen
or make it appear to swipe away.

Acceptance requires a real-widget route with multiple detected JS8Call
candidates: the initial ambiguity disables Next; selecting one candidate
persists that exact path, starts one replacement preparation, publishes the
same path in the retained JS8 draft, and enables Next. Assistant reflow/resize
coverage must also retain visible active content and footer controls.

## GRS-12 — Atomic VarAC Preparation In The Full Software Combination

### Production Reproduction And Scope

The binding route is `Settings > Radios > Add Radio > Software`: select
TriMode, FIO Spotter, CommStat, and the Fast Light/JS8Call bundle; select VarAC
and explicitly choose **Create VarAC cluster**. Production showed all detected
applications while VarAC remained internally unprepared, leaving `Next:
Connections` disabled with a generic reason. Repeated attempts also produced
brief UI stalls.

General Add Radio discovery and native VarAC planning are separate background
operations. Completion of the general discovery pass is not completion of the
selected VarAC family's native plan. The UI must represent that distinction as
one authoritative state per selected family, rather than exposing an
inconsistent page-level Ready status and a card-level unexplained block.

### Authoritative Family State And Navigation

Each selected family has exactly one current state: Preparing, Ready, Ready
with warning, Launch pending, Needs choice, or Blocked. The Software setup
status and its `Next` policy derive from those same states:

- while a native VarAC plan is pending, VarAC says **Preparing**, Software is
  not Ready, and Next is disabled;
- a qualified bundle is **Ready** and enables Next;
- `apply_requires_stopped_process=True` is **Ready with warning**, still
  enables Next, and clearly tells the operator to close VarAC and VARA before
  final Save;
- a non-ready native result is **Blocked**. Its exact native `why` is retained
  in the dialog draft and displayed on both the VarAC card and setup status;
  Next remains disabled.

This severity model does not weaken the no-damage boundary. Read-only native
planning is permitted while VarAC or VARA is running. The final Save transaction
is the only external apply boundary and remains responsible for requiring the
applications to be stopped, journaling, backup, transactionality, readback,
rollback, and durable FIO commit.

### Native Bundle, Discovery, And Selection Contract

For a managed VarAC family, Connection, Review, and launch rendering consume
only the already-qualified native bundle. They must never invoke legacy,
synchronous VarAC filesystem discovery. Detected application candidates are
published as one batched discovery result, once per current generation; they
must not trigger per-control rescans or GUI-thread scans while the operator
navigates.

Every selected-software combination uses the same state machine. The compact
matrix includes JS8Call only, Fast Light only, JS8Call + FIO Spotter + CommStat,
full TriMode without VarAC, and full TriMode with Create VarAC cluster. Valid
warning and launch-pending states do not block Next. Explicit safety failures,
missing required choices, and non-ready native plans remain visible blockers.

Deselecting VarAC immediately removes its draft and native-pending navigation
gate for the remaining prepared families. A late result from that removed or
otherwise stale generation must be ignored: it may not restore VarAC data,
change a card state, or disable Next.

### GRS-12 Acceptance And Exit Evidence

The automated implementation gate requires real-dialog coverage of every
matrix case above, delayed native preparation, qualified ready, qualified
ready-with-stop-before-final-Save warning, exact blocked-native reason,
stale-result rejection after deselection, and a failing legacy-discovery seam
to prove render/navigation stays bundle-only. `git diff --check` and the
applicable guided-radio/native preparation suites must also pass.

Implementation ownership and final automated evidence:

- primary `gpt-5.6-sol`, high reasoning: production-evidence review,
  architecture, native-plan lifecycle and freshness, transactional safety,
  delegated-diff review, integration corrections, and final gate;
- `gpt-5.6-terra`, medium reasoning: bounded real-dialog selection matrix,
  native-state regressions, and specification/work-log update;
- `gpt-5.6-luna`, medium reasoning: bounded production performance audit and
  hotspot call-chain analysis.

The exact GRS-12 focused suite passed **30 tests**. The broader guided-radio,
native preparation/writer/transaction, inventory, launch, and production-shaped
suite passed **180 tests**, and the Software Administration layout/assistant
suite passed **77 tests**. Changed Python compilation and `git diff --check`
pass. No schema or migration is introduced. Live Linux operator qualification,
including the reported full TriMode/Create-VarAC-cluster route, remains an
external release gate.

### GRS-12.1 — Wine Directory Aliases

Wine may expose an operator-owned Linux directory, especially
`drive_c/users/<user>/Desktop`, through a symbolic-link alias. A reviewed VarAC
BBS, archive, incoming, or outbox directory below that alias is not by itself a
native-writer safety failure.

FIO records the resolved destination of every managed data directory in the
immutable native plan. Final Save must verify that each alias still resolves to
that exact destination before creating anything, create only the reviewed
missing directories, and verify the resolved destination again afterward. A
broken alias, non-directory target, changed destination, or path outside the
reviewed lexical roots remains blocked before mutation.

This exception applies only to managed data directories. VarAC INI targets,
VARA source files, and managed VARA runtime/configuration targets retain the
strict rule that no path component may be a symbolic link. The plan fingerprint
includes the reviewed resolved data-directory destinations, so changing an
alias changes or invalidates the plan rather than silently redirecting it.

### GRS-12.2 — Stable Review Identity And Timed-Out Discovery Single-Flight

The binding production route is `Settings > Radios > Add Radio`: prepare a
managed VarAC cluster, review the prepared software plan, and select final
`Save Radio and Software`. The reported failure displayed **The prepared VarAC
plan changed or is no longer available** and sent the operator back to Software
even after another review of unchanged choices.

A preparation generation is a UI-lifecycle token, not transaction identity.
It fences publication of obsolete asynchronous results while an editor is
open. It must not invalidate an already reviewed transaction merely because an
automatic equivalent preparation completed later with a higher generation.
Final native apply resolves the newest cached ready preparation with the exact
reviewed immutable `plan_fingerprint`, then independently requires both of the
following to equal the reviewed `draft_fingerprint`:

- the draft embedded in the reviewed apply request; and
- the current live VarAC draft in Add Radio, or the current publisher draft in
  Software Administration.

Missing review fingerprints, a different immutable plan, edited topology or
policy intent, or a changed live draft still blocks before the native writer
starts. The existing final-writer process, source digest, target, Wine alias,
backup, journal, readback, rollback, and durable FIO transaction checks remain
unchanged. A rejected Add Radio save compensates any earlier guided native
configuration result before returning control to the operator.

A discovery timeout ends the UI's bounded wait; it does not stop Python code
already reading a JS8Call settings file. Identical phase work still running for
the same Add Radio session and scan-input fingerprint must therefore be reused
across newer UI generations. A newer generation may publish only its own
current combined snapshot, but it must not start a duplicate identical parser
while the earlier future is active. This single-flight rule limits the observed
CPU amplification without making file parsing synchronous or changing the
two-second partial-evidence budget.

Acceptance requires paired regressions at both native-apply entry points:
generation N review plus an equivalent generation N+1 cached preparation must
apply; a valid reviewed request followed by changed live intent must not start
another writer. Discovery coverage must prove that two same-session generations
which time out on one identical active JS8 profile scan invoke that scanner only
once. The VarAC acceptance, guided discovery/budget, and broader guided-radio
native suites, changed-file compilation, and `git diff --check` must pass. Live
Linux qualification of the exact Add Radio route remains the external gate.

Implementation ownership and automated evidence:

- primary `gpt-5.6-sol`, high reasoning: transaction/concurrency architecture,
  implementation, integration review, specification reconciliation, and gate;
- `gpt-5.6-luna`, low reasoning: read-only apply-boundary regression audit;
- `gpt-5.6-terra`, medium reasoning: read-only hotspot and discovery
  concurrency audit.

The focused VarAC/discovery suite passed **28 tests**. The broader Add Radio,
guided software, native preparation/writer/transaction, and save suite passed
**230 tests**. The Software Administration assistant/persistence/layout suite
passed **141 tests**. Changed Python compilation and `git diff --check` pass.
No schema or migration is introduced.

### GRS-12.3 — Existing VarAC Data-Root Inheritance And Post-Commit Planner Handoff

The binding production route is `Settings > Radios > Add Radio`: create or join
a native-managed VarAC cluster, select **Open scheduler** for Daily + Nets, and
complete final Save. Two independent defects were observed:

- FIO placed the new member's incoming and outbox folders below
  `.freqinout/managed-instances` even though the reviewed existing VarAC member
  already established an operator-owned VarAC data area; and
- Add Radio synchronously constructed Plan Builder while its
  `BEGIN IMMEDIATE` guided-save transaction was still open. Plan Builder's
  passive source projection attempted a Settings write, waited about five
  seconds on that transaction, then failed with `database is locked` on the UI
  thread.

For Create Cluster and Join Cluster, new incoming/outbox values are FIO-derived
facts, not repeated operator questions. The derivation order is:

1. retain a path the operator actually corrected after opening Advanced;
2. otherwise, when the reviewed source member has incoming/outbox evidence,
   inherit the corresponding parent directory and generate distinct,
   filesystem-safe `<radio-name>_In` and `<radio-name>_Out` children;
3. use the known sibling's parent for a missing incoming or outbox counterpart;
4. the 2026-09-19 implementation originally retained a conservative per-radio
   private managed-root fallback when no evidence existed. GRS-13 supersedes
   that fallback with one visible operator-selected VarAC data-area base.

All profile mailbox claims participate in deterministic collision avoidance.
FIO adds a stable numeric suffix rather than reusing or nesting another
member's directory. Incoming, outbox, BBS, and BBS archive remain four distinct
resources; BBS and archive stay cluster-shared while incoming/outbox stay
member-local. The reviewed exact mailbox targets, including stable Wine
directory-alias destinations, are included in the immutable native plan and
the existing no-damage writer checks. Existing directories or contents are
never moved, renamed, cleared, or deleted. A value published by an older native
preparation is generated evidence, not an operator override; re-prepare must
replace it when better reviewed source evidence is available.

Guided Save owns only database and native-configuration persistence. It may
record a requested planner handoff while the transaction is open, but it must
not navigate, construct a lazy tab, run scheduler queries, or write Settings
until the transaction has exited successfully. After commit, Add and Edit
Radio queue one next-event-loop Plan Builder handoff with the saved radio and
schedule choice. Rollback or failed Save queues nothing. Plan Builder's passive
constructor/header projection may update widgets from a saved plan but may not
persist source selectors; only explicit operator selection or an explicit
guided handoff may write those preferences.

The regression gate must prove:

- create- and join-cluster preparation inherit the reviewed mailbox parent on
  Windows-shaped and Linux/Wine paths;
- generated names are distinct and deterministic, actual Advanced corrections
  win, and an earlier FIO-generated managed-root value is re-derived;
- stable Wine Desktop aliases retain exact resolved-target safety, while broken
  or retargeted aliases remain blocked;
- Add and Edit Radio queue Plan Builder only after successful transaction exit,
  and any failed/rolled-back Save performs no navigation;
- passive Plan Builder source projection performs zero Settings writes; and
- the applicable native preparation/writer/transaction, guided-radio,
  planner, compilation, and diff-hygiene suites pass.

No schema or migration is introduced. Live Windows and Linux/Wine operator
qualification of the complete cluster Save and Daily + Nets handoff remains an
external release gate.

Implementation ownership and automated evidence:

- primary `gpt-5.6-sol`, high reasoning: evidence diagnosis, architecture,
  implementation, delegated-audit review, specification/work-log
  reconciliation, and final gate;
- `gpt-5.6-terra`, medium reasoning: read-only scheduler transition and CPU
  audit; and
- `gpt-5.6-luna`, low reasoning: read-only VarAC placement and no-damage audit.

The focused correction suite passed **18 tests**. The full applicable
guided-radio, Add Radio, planner, Software Administration, native
preparation/writer/transaction, and VNC partition passed **288 tests**.
Changed Python compilation and `git diff --check` pass.

### GRS-12.4 — Discovery Completion Handoff Ownership

The Add Radio discovery worker's successful `finished` payload and its
`QThread.finished` lifecycle notification are separate queued deliveries to the
GUI thread. Qt does not guarantee that the result relay is delivered first.
Thread teardown therefore owns only the worker/thread wrappers; it must not
remove the dialog completion callback. The result or failure handler alone
consumes that callback and transitions the current generation out of
`Discovery in progress`.

This remains true when all discovery phases finish quickly, the thread exits
before the GUI event queue drains, or unrelated startup/background activity is
using the UI thread. A late current result must still populate the prepared
plan. A stale/cancelled result follows the existing generation-fenced refresh
route. Missing callback ownership is logged as an internal diagnostic and may
not fail silently. Dialog/application shutdown may explicitly cancel and clear
outstanding callbacks after its bounded worker shutdown.

Acceptance requires a deterministic ordering regression that delivers
`QThread.finished` before the queued successful result and proves that the
callback still runs exactly once. A companion failure case proves the failure
handler consumes its callback exactly once. The prepare-first and
production-shaped guided Add Radio widget suites must continue to pass. No
schema or migration is introduced.

## GRS-13 — Application-Native Storage, Unified Administration, And Stable Multi-Instance Launch

Status: controlling specification accepted. The GRS-13.1 canonical identity,
GRS-13.2 layout, GRS-13.3 JS8Call native-identity parity, GRS-13.4 Fast
Light native-bundle parity/planner-handoff, and the GRS-13 Fast Light
FLMsg/FLAmp launch-isolation slice are implemented with
automated coverage; the wider native-writer, Station Message
Library, publication/NAS, and live Windows/Linux-Wine GRS-13 exit gates remain open. This section unifies
Add Radio and Software Administration for every supported software family. It
supersedes earlier language that:

- makes `.freqinout/managed-instances` the normal or final location for native
  third-party profiles, messages, databases, queues, inboxes, outboxes, or
  operator content;
- treats FLMsg or FLAmp as generically station-shared merely because their
  executable is shared;
- permits Add Radio, Software Administration, persistence, or Launch Control to
  reconstruct different representations of one software instance; or
- makes a radio link, family row, or launch manifest an independent source of
  truth rather than a projection of one canonical bundle.

Earlier implementation evidence remains historical evidence. New preparation,
save, reload, launch, reconciliation, and migration work must satisfy GRS-13.
The existing no-damage, qualified-writer, structured-command, rollback, RF
Guard, receive-only, concurrency, and responsive-UI contracts remain binding.

### Operator Mental Model

FIO presents five different things without conflating them:

1. **Installed application:** a normally station-shared executable or
   installation, such as one FLMsg, FLAmp, FLDigi, JS8Call, or VarAC install.
2. **Radio software instance:** the profile, data roots, endpoints, launch
   identity, and process components used by one radio. Concurrent instances are
   distinct even when they use the same executable.
3. **Station Message Library:** the canonical station-owned collection of
   completed received/imported content. Each receipt retains immutable
   radio/application/source provenance plus operating-group context metadata.
4. **Station publication service:** FIO Spotter FLAmp Q and the FIO BBS are
   station-scoped services over selected canonical messages. Each service has
   independent per-item publication state, access rules, audit, index, and
   eligible response-radio bindings.
5. **Operating-group context:** an operating group is receipt/routing metadata,
   a user filter, and a possible access-policy subject. It does not own the
   message, publication service, service index, native profile, endpoint, or
   response process.

The concise operator rule is:

> Application instances belong to radios. Received content belongs in the
> station message library. FIO Spotter FLAmp Q and FIO BBS are station services.
> Operating groups describe context and may grant or deny access.

Two radios in the same or different operating groups keep separate application
state, endpoints, receive locations, and launch identities while contributing
completed content to the same canonical station library. Library views may
filter by radio, software, operating-group context, sender, recipient, or access
policy. Identical content may be deduplicated without erasing any radio receipt
event or its historical group context.

Every path, endpoint, resource claim, launch component, canonical message, and
service binding carries exactly one scope and owner ID. Operating-group context
and ACL subjects are separate metadata, not ownership:

| Scope | Owns | Must not own implicitly |
| --- | --- | --- |
| Radio | native profile/data identity, endpoints, private logs, receive sources, member inbox/outbox, CAT/PTT authority, radio-scoped process | another radio's profile, endpoints, runtime state, or publication service |
| Operating group | group definition, membership, receipt-context label, saved filter, and access-policy subject | message ownership, publication service/index, radio CAT/PTT authority, native profiles, or endpoints |
| Station | canonical message library, FIO Spotter FLAmp Q, FIO BBS, installations, explicitly shared processes, catalogs, archive/synchronization and publication policy | loss of per-radio receipt provenance or implicit transmit authority |

Matching paths, executable names, ports, display names, or operating-group
membership never imply shared ownership.

### Managed Without Lock-In

`Managed by FIO` means that FIO prepares, validates, records, launches,
monitors, and reconciles a qualified configuration. It does not mean that FIO
hides operator files in an FIO-private location or may delete them.

Native application files and operator content remain in application-standard,
existing operator-established, or explicitly selected locations. FIO's private
configuration root may contain only FIO databases, bounded caches, indexes,
logs, journals, recovery evidence, and other replaceable/internal metadata.
The only copy of a native profile, received message, attachment, archive, Q
file, BBS file, or operator-created content must never exist solely below the
FIO private configuration root.

FIO establishes one visible local **Station Message Library**. It adopts an
existing operator library when selected; otherwise it proposes a recognizable
folder in the platform's ordinary user documents/data area and asks once for
confirmation. Radio provenance and operating-group views are metadata and
indexes over that library, not hidden duplicate directory trees.

Removing a radio, disassociating software, disabling launch, or uninstalling
FIO never deletes, moves, renames, clears, or makes inaccessible a third-party
profile or operator file. Deletion or migration is a separate, explicit,
reviewed operation outside Add Radio. A managed workspace may include a small
human-readable inventory explaining its radio, application, paths, and launch
selector, but that inventory is not required to use the application without
FIO.

### Native Directory Selection And Existing-Station Adaptation

Discovery occurs before any directory proposal. It inventories bounded known
application locations, saved FIO bundles, application launchers/shortcuts,
supported selectors, and existing multi-instance conventions without writing.
Each candidate is classified as one of:

- **Use existing instance unchanged**;
- **Create a distinct sibling instance**;
- **Clone as distinct** through a separately qualified reviewed operation;
- **Connect manually or remotely**;
- **Use an explicit station service**; or
- **Diagnostic only** because identity or ownership is incomplete.

For every newly created native path, FIO applies this precedence:

1. an explicit operator-selected and reviewed native directory;
2. the complete verified layout of an explicitly adopted existing instance;
3. the application's qualified platform-standard native location and naming
   behavior, including a profile or rig selector that causes the application to
   create its own standard path;
4. the verified parent and naming convention of a compatible existing sibling,
   with a new stable collision-free child; and
5. when no trustworthy convention exists, one visible operator-selected
   **Radio Software Data Folder**, prefilled from the platform's ordinary user
   documents/data area and remembered as station policy.

There is no `.freqinout/managed-instances` fallback for native application or
operator data. If FIO cannot qualify a native location, it preserves the
prepared bundle as `Needs choice` or `Saved; launch setup pending` and asks one
concise location question. It does not invent an opaque location.

A compatible sibling supplies only its application/platform/version convention
and parent. It never supplies another instance's child path, endpoint, profile,
message root, database, selector, environment, or launch identity. New child
names use the immutable instance key for collision resistance while displaying
the operator's radio name. A later radio rename changes the display label only;
paths, selectors, ports, and launch identity remain stable unless the operator
performs a separate reviewed relocation/rename.

Existing single- and multi-instance installations are first-class:

- adoption stores the complete observed bundle unchanged and source-locks it;
- an established multi-radio parent/naming convention is reused for a new
  sibling after compatibility and collision checks;
- existing folders, files, launchers, shortcuts, ports, and process
  environments are never normalized or silently replaced;
- discovery ambiguity produces a clear choice, not a best-guess assignment;
  and
- a shared legacy receive root is labeled **Station-shared; radio attribution
  may be limited** rather than falsely assigned to a radio.

Review shows the selected base, every derived child, why FIO chose it, whether
FIO manages or only watches it, and that no existing file changes before final
Save.

### Canonical Recipe Directory Ownership And Creation

The reviewed canonical recipe is the sole authority for every directory that
FIO creates. The same accepted recipe is persisted, then used by both Add Radio
and Software Administration for every application family; no screen, family
row, manifest projection, launch planner, legacy default, or filesystem guess
may derive an additional FIO-created directory.

Final Save creates only the reviewed canonical-recipe directories, and creates
them before the transaction commits or any launch is attempted. A launch
preflight may idempotently repair a missing directory from that same persisted
authority for an older saved managed instance; it may not derive a new path.
The resulting canonical recipe records the exact created-directory set and its owners so a
later reload, manual launch, startup launch, reconciliation, or Software
Administration session uses that same set without inventing another path.

This authority is deliberately narrow:

- an existing directory remains in place with its contents untouched by the
  directory-preparation step; a separately qualified, reviewed native writer
  may still update its exact file target under the normal backup/readback/
  rollback contract;
- an operator-selected path, an adopted path, and an intentionally shared
  station/application path remain operator- or shared-owned unless the reviewed
  recipe explicitly identifies a distinct new child for FIO to create; and
- a file target is never created as a directory. This includes executables,
  configuration/INI files, databases, logs, message files, archives, launchers,
  shortcuts, and any other file-path target.

If the reviewed recipe does not identify a creatable directory or an existing
parent needed for a file target, FIO preserves the bundle as `Needs choice` or
`Saved; launch setup pending`; it does not infer a directory from the target.
These rules apply identically to the Add Radio route and the standalone
Software Administration route for every supported app family.

The current family mapping is explicit: managed JS8Call creates its native Qt
settings parent and radio-scoped data/save/forms directories; managed FLRig,
FLDigi, and FLMsg create their native radio-name profile and data roots; a
reviewed Fast Light recipe may create its exact station-standard FLAmp receive
and transmit children; and a qualified managed VarAC create/join transaction
creates its exact member mailbox, shared BBS/archive, INI-parent, and distinct
VARA-runtime directories. CommStat and FIO Spotter are station services and
authorize no application directory. Adopted/read-only software authorizes no
directory creation.

### Family-Specific Native Layout Contracts

#### JS8Call family

FIO uses the exact application/version-qualified rig or profile selector and
the native standard data location that selector creates. A distinct instance
has one stable rig name, settings path, application-data root, SaveDir/forms
root, API/UDP endpoints, structured command, working directory/environment,
and radio assignment. Those facts share one immutable instance key and persist
unchanged through Software Administration and Launch Control. FIO never pairs a
new API port with an existing profile or stores a draft-key path while launching
a final-key selector.

For stock JS8Call, Improved, and Subspace variants that follow the upstream
multi-settings contract, the reviewed radio-derived rig name is the single
human-facing native identity. FIO passes that value with `--rig-name`. JS8Call
then uses `JS8Call - <rig name>` as its Qt application name; FIO must therefore
derive the settings file and writable application-data root from that same
application name and the platform's Qt standard locations. Examples are
`~/Library/Preferences/JS8Call - FT-710.ini` plus
`~/Library/Application Support/JS8Call - FT-710/` on macOS,
`~/.config/JS8Call - FT-710.ini` plus
`~/.local/share/JS8Call - FT-710/` on conventional Linux, and the
corresponding Qt local configuration/data locations on Windows. `DIRECTED.TXT`,
`ALL.TXT`, `inbox.db3`, Save, and forms resources are children of or explicitly
linked to that native application-data identity. A qualified adopted instance
retains its observed native locations unchanged.

`--rig-name` and `--config` are not interchangeable. `--rig-name` selects the
distinct application name, lock, settings file, and data identity. `--config`
selects an alternate MultiSettings profile inside that already-selected
settings file. The normal distinct-instance recipe uses `--rig-name <radio>`
and writes the reviewed values to that file's active `Configuration` group; it
must not write only `MultiSettings/<radio>` and then omit `--config`, because
those prepared values would be inactive. FIO never writes the new radio's
values into the detected default `JS8Call.ini`; that path is source evidence
only. An explicit alternate-profile workflow must persist and launch with both
selectors and is a separately qualified recipe.

The Add Radio transaction key and draft instance key are internal concurrency
identifiers only. They must never appear in `--rig-name`, a JS8Call application
name, settings filename, application-data directory, message path, Software
Administration title/body, or launch summary. When the draft is created, FIO
also allocates one separate durable application key. That durable key is shared
by the application row, manifest, launch projection, and canonical identity
record, while the operator-readable radio-derived rig name independently drives
JS8Call's native paths. Final Save may not recompute either identity. A radio
rename preserves both until the operator performs a separate reviewed native
identity rename/relocation.

Software Administration displays the radio/instance label and native JS8Call
resources. Opaque application, canonical, fingerprint, and transaction keys
remain available to diagnostics but are not normal user instructions. Existing
rows created by an older release with a draft-derived rig name are reported as
`Needs attention`; FIO does not silently rename or move their files. The
operator uses the reviewed **Replace instance** recovery transaction to adopt
or create the correct native identity without changing another JS8Call
instance.

#### Fast Light core

An installation/executable may be station-shared; radio runtime state is not.
Each managed radio receives distinct qualified FLRig and FLDigi configuration
roots, endpoints, logs/check-in paths, audio/control bindings, component keys,
and structured launch recipes. FLRig and FLDigi use their documented selectors
and remain paired to the same radio. A shared binary never causes process-name
deduplication of radio-scoped components.

Launch suppression is instance-scoped, not application-name-scoped. For
FLRig, FLDigi, and JS8Call, the persisted application identity and configured
service endpoint are authoritative even when a selected-radio launch plan
contains only one row. A same-named process owned by another radio must not
cause FIO to wait on the selected radio's absent endpoint. FIO launches the
selected persisted recipe unless its exact executable-and-arguments process is
already running; an exact process whose endpoint is still starting is polled
and is never duplicated.

For a new managed instance, the final native roots are derived before Review
from the final radio label alone. The display name may contain spaces, while
the native child is a portable radio-name slug using letters, digits, periods,
underscores, and hyphens; whitespace and other separators become hyphens. The
immutable durable application key remains in FIO persistence and is forbidden,
along with the transaction-only `draft-fast_light-*` key and FIO private
configuration/runtime roots, in every final FLRig/FLDigi/NBEMS path. With no
reviewed station override, Unix/macOS use distinct children below the native
`~/.flrig`, `~/.fldigi`, and `~/.nbems` conventions; Windows uses the qualified
equivalent native application/NBEMS locations. An established reviewed native
base takes precedence. Final Save must not recompute these roots.

Selecting a Fast Light component is a persistence requirement, not merely a UI
checkbox. If FLRig, FLDigi, FLMsg, or FLAmp is selected and its reviewed
executable is available, Final Save atomically writes all of the following or
writes none of them: component selection flag, executable, native resources,
endpoint/arguments/dependencies, application row, manifest claims, launch row,
canonical identity record, and Software Administration projection. A launch
recipe that knows an executable while the saved radio row is blank is a failed
transaction and must never be reported as Ready.

Selection and discovery are separate inputs. The explicit component selection
is authoritative; a discovered FLMsg or FLAmp executable must not add that
unselected component to the radio. Conversely, a selected component whose
executable cannot yet be qualified remains present in the canonical bundle as
`Saved; launch setup pending`, with its application-standard derived resources
and one Browse/recovery action. Discovery failure may not silently erase the
selection or the component identity.

#### FLMsg

For managed multi-radio operation, FLMsg data is radio-scoped by default even
though the executable is normally shared. FIO uses the application's supported
`--flmsg-dir <radio NBEMS root>` selector as FLMsg's complete structured launch
identity. FLDigi, not FLMsg, receives the matching `--flmsg-dir <radio NBEMS
root>` and `--auto-dir <radio WRAP/auto>` values for that radio's handoff
workflow. Current qualified FLMsg releases advertise `--auto-dir` in help but
do not parse it; FIO must not pass that unsupported argument to FLMsg. The
bundle persists, at minimum, the root plus `ICS/messages`, `ICS/templates`, and
`WRAP/auto` resources. FIO derives these from the native convention; the normal
flow does not ask the operator to type each child path.

FLDigi and FLMsg persist the same reviewed NBEMS root and `WRAP/auto` path.
FLDigi launches with both selectors; FLMsg launches only with
`--flmsg-dir <radio NBEMS root>`. Software
Administration must show the exact saved executable and `ICS/messages` source
that those arguments select; it may not show a blank field, an unrelated
station default, or a draft-only path.

FLMsg has no service endpoint readiness substitute. FIO therefore recognizes
an already-running managed FLMsg instance only by its exact executable and the
persisted `--flmsg-dir` argument identity. Another radio's FLMsg
process does not satisfy that check. A legacy FLMsg row without a qualified
argument identity remains on the compatibility/operator-review path; FIO does
not guess and spawn a potentially conflicting second process.

FLMsg's XML-RPC address and port are native per-root preferences, not launch
arguments. FIO may show the intended FLDigi endpoint as review evidence, but it
must not claim to have configured that endpoint until a platform/version exact
`FLMSG.prefs` writer has completed backup, write, readback, and rollback
qualification. This does not block safe creation of the radio-scoped root or
launch with `--flmsg-dir`; it is a visible post-launch review item when native
preferences have not been qualified.

An intentionally shared FLMsg workspace is an explicit advanced choice with a
declared station owner, concurrency policy, radio bindings, and limited
attribution. Operating groups may appear in its message ACLs but do not own the
workspace. Sharing an executable never shares its NBEMS root.

#### FLAmp

Each concurrent FLDigi/FLAmp pair has distinct, persisted XML-RPC and ARQ
endpoints, component keys, dependencies, and launch identity. The current
qualified FLAmp contract uses one radio-scoped NBEMS root plus the structured
arguments `--config-dir <radio NBEMS root>`, `--arq-server-address <IPv4>`,
`--arq-server-port <radio ARQ port>`, `--xmlrpc-server-address <IPv4>`, and
`--xmlrpc-server-port <radio FLDigi XML-RPC port>`. FLDigi receives the matching
ARQ address/port arguments. Because the qualified FLAmp parser accepts dotted
IPv4 addresses rather than arbitrary host names, a managed local recipe uses a
reviewed numeric loopback or numeric local address.

FIO allocates the ARQ port independently from FLRig and FLDigi XML-RPC ports,
checks all three for overlap, and persists it in the canonical manifest,
Software Administration projection, Add Radio review, and launch rows. FIO stores
the exact native receive source and an explicit outgoing staging/queue resource;
it never derives a transmit directory merely by replacing `rx` with `tx`.

The shared executable installation never makes FLAmp process state or receive
storage station-shared. The per-radio NBEMS root owns its `FLAMP/rx`,
`FLAMP/tx`, `FLAMP/scripts`, and `FLAMP/relay` children. Final Save creates only
those reviewed directories, before launch; launch preflight may idempotently
repair only the same persisted set. Another radio's FLAmp process cannot satisfy
readiness because exact executable plus all persisted arguments are the process
identity. Launch-at-startup follows the radio's Launch Control policy and still
requires **Use Radio**.

Every Launch Control entry point, including the row-level **Start** action and
an unsaved in-memory Launch Control draft, must rehydrate immutable component
recipe fields from that selected radio's committed canonical identity before
planning. Rehydration restores the component key, executable, exact argument
vector, working directory, dependencies, readiness evidence, and the reviewed
managed-directory set while preserving the operator's enabled,
launch-at-startup, and monitor checkboxes. It must not borrow a same-named
process or recipe from another radio. A canonical component absent from the
in-memory draft may be restored for review but remains disabled and excluded
from that launch. A qualified FLAmp row may be launched
only with its complete radio-scoped native argument identity; a stale row that
cannot be rehydrated is rejected with a specific missing-identity message
before process inspection or launch. Launch preflight may idempotently recreate
only the persisted managed directories for that recovered recipe and never
derives or creates a new path.

An adopted existing launcher may preserve another proven platform/version
layout unchanged. A legacy FLAmp row lacking a qualified root or explicit ARQ
pair remains `Needs attention` and is never silently converted, shared, or
started. Recovery uses one reviewed Replace-instance transaction. Missing or
ambiguous minor details may allow the radio/software draft to save as
`launch setup pending`, but FIO must not invent isolation or let another radio's
process satisfy it.

### GRS-13.4 — Fast Light Projection And Planner Handoff Invariant

The authoritative Fast Light bundle is prepared once and projected everywhere.
The component recipe is the recovery source for derived paths when the generic
details assistant intentionally hides them. Final Save rehydrates manifest
claims from that reviewed recipe before the database transaction and then
reads the saved application/manifest identities back into the canonical record.
The bundle records the explicit selected-component set independently from the
discovered executable evidence; projections may not reconstruct selection from
whether a path happens to be blank or nonblank.
Consequently, Add Radio Review, the linked Fast Light row, Software
Administration, Launch Control, and canonical parity validation must agree on:

- the durable manifest/bundle key and `fio_managed` ownership;
- FLRig/FLDigi executables, distinct native configuration roots and XML-RPC
  endpoints;
- FLDigi log and NBEMS auto/check-in sources;
- FLMsg executable, NBEMS root, messages, templates, auto path, arguments and
  dependency;
- FLAmp executable, radio-scoped NBEMS root, receive/outgoing sources,
  explicit ARQ/XML-RPC pairing, startup policy and dependency; and
- every structured component command, working directory, readiness fact and
  resource claim.

The save adapter may not default a `mode=managed` or `ownership=fio-managed`
draft to an operator-managed manifest. The persistence boundary must infer the
explicit management mode and preserve it on reload.

When Add Radio saves a radio and hands the user to Plan Builder before a plan
assignment exists, the just-saved radio ID is the explicit RF Guard context.
Plan save/review validates against that radio and must not display the generic
"No radio context" warning. The same retained ID is passed to the subsequent
Assign Schedule action.

Legacy rows containing draft-private native paths, blank selected components,
or split ownership remain readable and `Needs attention`; FIO does not silently
move or overwrite their external data. **Replace instance** is the reviewed
recovery route. New Final Save and Replace transactions must satisfy this
invariant without requiring a migration of unrelated existing rows.

#### GRS-13.4a — Fast Light component-scoped legacy repair

For valid Fast Light identities with legacy or incomplete FLMsg/FLAmp data,
Software Administration MUST expose **Repair FLMsg/FLAmp components**, a
narrower action than Replace instance. It preserves FLRig/FLDigi executable,
native profile, endpoint, and launch identity; only additive FLDigi ARQ-pairing
arguments needed for FLAmp may be introduced. Unrelated families and Launch
Control preferences remain unchanged. No external application files may be
written. One optimistic generation-checked atomic transaction merges only
FLMsg/FLAmp canonical, manifest, and launch rows, then performs complete
readback/parity validation and rolls back on any error.

### GRS-13.5 — Atomic Publication And Edit-Apps Invariant

The Add Radio database transaction and the UI publication boundary are the
same generation boundary. A reviewed save may create the radio row first as an
internal transaction step, but no Settings consumer may reload, emit a radio-
inventory change, rebuild Software Administration, or present that provisional
row until all selected application rows, manifests, launch records, canonical
software identities, station-service bindings, operating-model assignment,
and optional VarAC cluster membership have succeeded and the outer transaction
has committed.

This rule applies equally to Add Radio and Edit Radio:

- while the transaction is open, Radios, Edit Apps, Software Administration,
  Launch Control, readiness, and runtime projections retain the last committed
  generation;
- after commit, FIO performs one authoritative reload and emits one inventory
  change so every consumer sees the complete new generation;
- after rollback, FIO performs the same authoritative reload of the retained
  generation; it may not leave a provisional radio card, stale checkboxes, or
  an unsaved workspace that appears to be the failed radio;
- a Tri-Mode selection with FIO Spotter and CommStat means the committed radio
  has canonical JS8Call, complete selected Fast Light, VarAC, FIO Spotter, and
  CommStat identities. The VarAC topology selected in Review is part of that
  same all-or-nothing save; and
- a selected radio must appear in Software Administration for every selected
  family immediately after a successful save and after restart. Absence from
  Software Administration is proof that the save did not commit, not a
  permissible delayed projection.

**Edit Apps** is an entry point to the same guided Software step used by Add
Radio. It is not an independent set of legacy boolean toggles. For a canonical-
backed radio, compact software checkboxes are read-only summaries; changing the
stack reopens the reviewed assistant and saves component selection, native
paths, endpoints, manifests, station-service bindings, launch recipes, and
canonical identities together. Internal persistence instructions such as
`adopt_software_instance(..., replace_existing=True)` must never be exposed as
operator recovery guidance.

Acceptance requires a transaction-order regression proving that refresh and
public inventory signals occur only after commit/rollback, a failed-save case
proving no provisional radio remains visible, an Edit Apps route test proving
it opens the guided Software step, and canonical identity coverage for the
Tri-Mode + FIO Spotter + CommStat stack. The focused guided-final-apply,
Software Administration, canonical-identity, VarAC topology, and changed-file
hygiene gates must pass.

### GRS-13.6 — Portable Native Names And Guided Step Geometry

Human-facing radio and instance labels may contain spaces and normal operator
punctuation. A qualified recipe keeps that readable display label but derives
application-native directory/profile segments with a conservative portable
slug. For Fast Light the slug is exactly the sanitized radio name; no draft
key, application-system-key digest, random token, or FIO-private directory is
appended. Thus `Field Radio 1` displays unchanged and uses `Field-Radio-1` in
the generated FLRig, FLDigi, and radio-scoped NBEMS roots. Existing reviewed
explicit paths are preserved; this rule governs newly prepared identities and
reviewed replacement/migration only.

The fixed Guided Add Radio header contains only purpose and step navigation.
It remains at its natural height when the dialog is maximized; the single body
scroll area receives the remaining space. Every Back, Next, or enabled step
selection resets that body scroll position to the top after visibility/layout
settles. In particular, entering Safety must show its antenna and supported-band
controls first rather than inheriting a lower Software/Connections offset.

Acceptance requires pure recipe coverage proving a spaced radio label produces
portable radio-name-only Fast Light roots with no durable-key fragment, plus a
real-widget geometry route proving the header has fixed vertical policy and a
step transition resets a deliberately bottomed-out body scroll to zero.

#### VarAC and VARA

GRS-10's install-adjacent unique VarAC INI and structured Windows/Wine launch
contract remains authoritative. Member-local incoming/outbox and runtime paths
follow an explicitly selected or reviewed existing VarAC/VaraFiles parent;
cluster BBS/database resources follow their declared cluster ownership. A new
member uses distinct stable children beside the established operator layout.
If no standard or existing layout can be qualified, FIO asks once for the
VarAC data-area base folder; it does not fall back to `.freqinout`.

#### FIO Spotter and CommStat

FIO Spotter remains a built-in station service. Its FLAmp Q publication catalog
and the FIO BBS are station-scoped over the canonical Station Message Library.
CommStat remains one explicit station process with per-radio JS8 endpoint
bindings. None creates duplicate radio-owned executables or changes message
ownership merely because it observes or serves content.

### Local, Removable, And Network Storage

Every persisted storage resource records its normalized path, scope/owner,
read/write capability, storage class (`local`, `removable`, or `network`),
filesystem/mount identity when available, verification evidence, mutation and
retention policy, and local-cache/synchronization policy where applicable.

The safe default is local-first:

- mutable native profiles, databases, lock files, partial receive state,
  autosend queues, active inbox/outbox, and application runtime state stay on a
  qualified local filesystem;
- completed-message archives, station publication content/index replicas, BBS
  content, FLAmp Q content, templates/reference material, exports, and backups
  may use a reviewed NAS or removable location; and
- an application/version may use network-backed mutable state only through an
  explicit qualified recipe that proves its locking, atomicity, availability,
  and recovery semantics.

A reachable mount is not proof that mutable application state is safe. Network
credentials remain owned by the operating system and are never stored in the
bundle. Changed mount identity, resolved target, or permissions makes the
affected resource stale and prevents automated writes until reviewed; FIO
never silently remaps it.

Reception and radio operation never wait on a NAS. FIO first accepts completed
content into a visible local station message library, then synchronizes or
publishes asynchronously using checksums, temporary-file-plus-atomic-rename
where the filesystem supports it, bounded retries, and idempotent records. A
NAS outage shows `Synchronization delayed`, retains queued work locally, and
keeps the last known-good local Q/BBS publication available. It does not freeze
the UI, drop the native receive event, clear an index, or block an unrelated
radio.

### Safe Message Intake, Presentation, And Delivery

Native application folders are sources, not the sole FIO message database.
FIO watches each declared source read-only unless a qualified recipe explicitly
owns an outbound handoff. A source scan:

1. rejects path escape and unexpected symbolic-link traversal;
2. waits for a regular file's size and modification evidence to stabilize;
3. applies bounded size/type handling and calculates a content hash;
4. imports or copies the completed content atomically into the visible local
   station message library;
5. records radio, software instance, source path, operating-group context,
   content identity, size, times, parse state, and authentication/signature
   evidence; and
6. never deletes or moves the native source automatically.

The Messages UI reads canonical imported records rather than trusting a live
mutable source file. It escapes untrusted text and filenames, sanitizes rendered
markup, does not execute attachments, and presents unknown/binary content as an
attachment. A changed file is a new revision. Repeated observation of identical
content is idempotent while preserving every distinct receipt/provenance event.

FIO-owned outgoing work uses a radio-scoped local outbox and an explicit
destination resource. Handoff to FLMsg, FLAmp, VarAC, BBS, or another service
uses a temporary file and atomic promotion where supported, records the result,
and never uses a receive folder as an inferred transmit queue. Any send,
automatic forwarding, queue enqueue, or purge binds to a transceiver-owned
preflight context and applicable RF Guard; receive-only radios may decode and
import but never gain transmit authority through a file path.

### Station Message Library And Access-Controlled FLAmp Q/BBS Publication

Receipt, publication, authorization, and response transport are separate
decisions:

1. traffic lands in the designated native location for the receiving
   radio/application instance;
2. FIO validates and imports the completed item into the canonical station
   message library with immutable receipt provenance;
3. the operator or an explicit automation rule publishes that canonical item
   to FIO Spotter FLAmp Q, FIO BBS, both, or neither;
4. each station service evaluates its own per-item access policy; and
5. an authorized request is assigned to exactly one eligible response radio and
   passes that radio's readiness, RF Guard, activity, and transmit preflight.

Newly received content is private and unpublished by default. Publication does
not move, duplicate, or transfer ownership of the canonical message. One
canonical item may have independent FLAmp Q and FIO BBS publication entries,
public names, retention, expiration, and access rules.

An access policy may include explicit callsigns, trusted operators, station
roles, one or more operating groups, all authenticated stations, an explicitly
selected public audience, expiration, and explicit deny/exclusion rules. Group
membership is only one authorization subject. A message received while radio A
was operating with group A may be shared with group B, several groups, named
callsigns, or another permitted audience without changing its receipt
provenance or copying it into group-owned storage. Group reassignment changes
prospective filtering/policy evaluation only; it never rewrites history.
Access is default-deny, explicit deny takes precedence over allow, and every
policy decision records the requester identity, matched rule, service, item,
time, and result.

FIO Messages shows receiving radio/application, receipt context, validation,
and independent publication states. The operator can select one or more
canonical messages and choose `Publish to FLAmp Q`, `Publish to FIO BBS`,
`Publish to both`, or `Remove publication`, then review each service's access
rules. Automatic publication is opt-in, bounded, auditable, and uses the same
policy engine.

FIO Spotter FLAmp Q and FIO BBS never serve arbitrary live receive folders,
generic global `message_paths.flamp`, or an operator filesystem tree. Each
serves only complete, validated canonical records with an active publication
entry. Each station service maintains a separate policy, audit log, and
generation-fenced local index keyed by stable content identity plus an
unambiguous public name, not by a mutable source path.

Publication and withdrawal indexes are atomically replaced after successful
validation. An incomplete file, failed NAS synchronization, or failed refresh
cannot erase the last known-good local index. A withdrawal becomes a durable
pending operation until every required local index is updated; the UI must not
claim that access was removed while a stale serving index can still answer.
Canonical content and receipt provenance remain after unpublish.

Response-radio selection is station-service policy over explicit eligible radio
bindings, not operating-group ownership. An active-responder lease/preference
selects one radio for a request and prevents duplicate replies. Authorization
does not itself grant a transport, frequency, PTT, or automatic-send right.

The **FIO BBS** described here is distinct from a native VarAC cluster BBS.
VarAC BBS/database/archive paths remain VarAC cluster-owned native resources.
Changing them does not relocate or reconfigure the FIO BBS publication catalog,
and FIO BBS ACL/index changes do not rewrite VarAC native configuration.

### One Canonical Bundle And Complete Persistence

Automatic preparation produces one immutable `AtomicInstanceBundle` per
selected family. Successful final Save persists one versioned canonical
`SoftwareIdentityRecord` per selected family, with stable bundle/component/
binding IDs and a complete fingerprint, in one generation-fenced radio identity
set. The application rows, manifests, and launch rows remain required verified
projections of that identity set.
This canonical bundle is the source of truth. Device profiles, family-specific
rows, cluster/membership rows, message-source rows, operating-group context and
ACL-subject bindings, station publication/ACL rows, storage-resource rows, and
launch manifests are deterministic projections, not competing authorities.

#### Mandatory Add Radio / Software Administration identity parity

Every software family, application component, built-in binding, and shared-
service binding that Add Radio selects, discovers, prepares, or reviews must be
present in Add Radio's embedded Software Administration editor and Review before
final Save, then in standalone Software Administration immediately after Save
and after restart/reload. A new unsaved radio is never materialized as a
standalone Settings row merely to satisfy this rule. Both embedded and
standalone surfaces project the same generation-fenced canonical draft/bundle;
they are not permitted to create parallel records.

This is an exhaustive rule, not an example list. The required canonical
projection for each selectable Add Radio capability is:

| Add Radio selection | Canonical family record | Required nested identities visible in Software Administration |
| --- | --- | --- |
| Receiver application / SDR++ | `sdrpp` | receiver component, receive-only scope, exact executable/argv/cwd/environment/dependencies/readiness, radio binding |
| JS8Call | `js8call` | JS8 application component, rig/profile selector, settings/data/message paths, API/UDP endpoints, radio binding |
| Fast Light | `fast_light` | every selected FLRig, FLDigi, FLMsg, and FLAmp component; none may be inferred from another component |
| VarAC | `varac` | VarAC and VARA components, node/cluster bindings, INI/database/runtime/incoming/outbox/BBS/archive resources, ports, dependencies |
| FIO Spotter | `fio_spotter` | explicit `fio-spotter` built-in component and built-in radio binding; selection is never inferred from JS8Call |
| External JS8Spotter | `external_js8spotter` | external process component, executable/recipe, JS8 endpoint dependency, and radio binding |
| CommStat | `commstat` | one `commstat:station` process identity plus the selected radio's distinct JS8 endpoint binding; never a duplicate process identity |

The canonical family key may differ from a human-facing label, but it must be
mapped explicitly and losslessly (`sdrpp` to **Receiver application** and
`external_js8spotter` to **External JS8Spotter**). Fast Light remains one
atomic family record whose selected children are individually persisted
components. Station-owned services remain explicit records with radio-specific
bindings. A checkbox, display label, or neighboring family is never sufficient
evidence that an identity exists.

For each selected identity, both surfaces must expose or carry the exact same:

- canonical bundle ID and immutable component or binding ID;
- family and component kind, including distinct FLRig, FLDigi, FLMsg, FLAmp,
  VARA, FIO Spotter binding, and CommStat endpoint-binding identities;
- radio/station owner, execution scope, management/source mode, completion
  policy, provenance, and verification state;
- native configuration, data, message, queue, database, archive, and storage-
  resource paths with the same normalized values and ownership;
- endpoints, selectors, executable, structured argument vector, working
  directory, environment, dependencies, launch policy, and readiness policy;
- desired/native evidence and the same complete parity fingerprint.

The UI may group these identities differently for the task at hand. It may not
omit one, duplicate one, append a surface-specific identity name, silently
rename one, independently edit one projection, or reconstruct one from a
neighboring family, radio, legacy default, display label, or filesystem guess.
FIO Spotter is an explicit built-in station-service binding; it is not implied
merely by the presence of JS8Call. CommStat is one explicit station process
with a distinct per-radio JS8 endpoint binding; it is not a second JS8Call
instance. Each radio identity set references the same durable
`commstat:station` process ID and contributes only its own endpoint binding;
repetition of that reference in per-radio canonical sets never authorizes or
creates another CommStat process or launch identity. Fast Light may have one
family summary, but FLRig, FLDigi, FLMsg, and
FLAmp remain individually identified canonical components beneath it.

An edit from Add Radio or Software Administration's **Add software instance… /
Replace instance** assistant updates the same draft generation and invalidates
older discovery/preparation results. Canonical-backed compact task fields in
Software Administration are a read-only projection; they must not offer an
independent partial Save. This is deliberate: identity, paths, endpoints,
manifest, and launch recipe are changed together through the instance assistant
or not at all. Final Save from either authoritative assistant uses the same
transaction. The committed radio links, compatibility rows, manifests, launch
rows, and Software Administration summaries are projections of that transaction
and must reload to the identical parity fingerprint. A missing, extra,
duplicate, stale, or mismatched identity is `Needs attention`; the affected
launch is blocked, and FIO offers reconciliation without changing an existing
native configuration automatically.

`Save as draft` persists no active radio link and performs no native write, but
it must retain the complete canonical identities so Add Radio's embedded
Software Administration editor can resume the same draft without value-by-
value copying. A saved inactive radio draft may then appear in standalone
Software Administration, still carrying the same identity IDs and fingerprint.

The final transaction writes and reload-verifies every required projection.
Projection parity includes:

- radio, operating-group, family, instance, and component identities;
- source/adoption mode, ownership, scope, and completion policy;
- normalized and resolved native paths plus storage classification;
- endpoints, selectors, executable, structured `argv[]`, working directory,
  environment, dependency order, launch/readiness policy, and execution scope;
- message sources/destinations, operating-group context/ACL-subject bindings,
  station Q/BBS publication and access-policy resources, and
  retention/synchronization policy;
- desired/native fingerprints, writer/recovery evidence, and verification
  state.

Add Radio, Software Administration, Connections, Review, Health, Messages,
Spotter, scheduler, Launch Control, startup, and reconciliation load the same
canonical bundle or one verified projection carrying its bundle/component ID
and fingerprint. No consumer reconstructs missing fields from legacy defaults,
adjacent records, display text, or filesystem guesses. Missing or mismatched
projection is `Needs attention` and launch-blocking for the affected component;
it is never silently repaired from another radio's row.

Draft preparation and `Save as draft` are non-mutating. Final Save from either
entry surface invokes the same qualified rollback-capable native/FIO
transaction. The accepted bundle is applied once, persisted once, and published
once. Cancel, stale generation, source drift, injected writer failure, database
failure, or projection readback failure restores the prior native and FIO state
and leaves no active orphan.

### Stable Multi-Instance Launch

Each external process identity has one persisted component manifest containing
its stable component key, executable, exact `argv[]`, working directory,
environment, dependencies, execution scope, radio/group/station owner,
readiness target, and resource claims. Display command text is rendered from
that vector and is never reparsed.

Process-name or executable-path deduplication is forbidden. Deduplication is
permitted only when the components carry the same intentionally shared durable
component key and compatible scope/bindings. Separate radio components launch
separately even when they use the same binary. Manual launch and startup use
the same persisted recipe. Reload, radio rename, UI navigation, or a later
discovery pass cannot change the selector, paths, endpoints, or arguments.

Launch order and readiness are component-specific. A slow or failed component
stays within its endpoint lane and cannot delay another radio. Readiness proves
only the declared process/service evidence; it does not infer radio control,
message attribution, or transmit authority.

### Unified Add Radio And Software Administration Experience

Add Radio remains the normal radio-first flow. Its Software step:

1. shows selected capabilities and required source/topology decisions;
2. discovers existing application/native-layout evidence asynchronously;
3. proposes adoption or a distinct sibling using the directory policy above;
4. prepares the complete bundle automatically;
5. shows one concise card per family with status, ownership, radio scope,
   operating-group context where relevant, location summary, launch policy, and
   one recovery action; and
6. continues without opening Software Administration when the bundle is ready
   or ready with a non-safety warning.

Software Administration is the same bundle editor entered software-first or as
an exception/review route. It never starts with a blank generic Files page when
preparation can derive values, never loses a prepared Add Radio value, and never
asks for recipe-owned child paths or commands. Its normal summary separates:

- **Application installation** — normally shared executable/install;
- **Radio workspace** — profile, data, endpoints, receive/outgoing resources;
- **Messages and publication** — station library intake, FLAmp Q/FIO BBS
  publication state, access policy, and eligible response radios;
- **Operating-group context** — receipt/filter metadata and available ACL
  subjects, not service ownership;
- **Station storage** — archive/NAS synchronization; and
- **FIO action** — manage, watch unchanged, publish, synchronize, or operator
  starts.

Generated values are read-only with `Why` and `Show details`. Advanced permits
an intentional correction or manual source. A corrected value changes the
canonical draft and triggers one generation-fenced reprepare; it never patches
one projection independently.

Both surfaces use identical labels, readiness severity, review text, and Save
semantics. A successful Software Administration `Save as draft` returns the
complete non-mutating bundle to Add Radio. Standalone Software Administration
final Save invokes the same final transaction and persists the same bundle.

The division of responsibility remains visible and concise:

| Operator decides | FIO derives and performs when qualified |
| --- | --- |
| radio name, role, and FIO Behavior | stable internal identity and radio-scoped capability limits |
| use existing unchanged, create distinct, manual/remote, or explicit shared service | bounded discovery, compatible sibling selection, native directory proposal, and collision checks |
| operating-group membership/context | receipt provenance, saved filters, and ACL-subject resolution without service ownership |
| publication to FLAmp Q, FIO BBS, both, or neither; audience and expiration | station-scoped publication entries, policy evaluation, audit, indexes, and one eligible response-radio route |
| VarAC standalone/create/join topology and genuine ambiguous source choice | member paths, ports, native files, structured launch, dependencies, and rollback plan |
| optional NAS/archive destination and retention policy | local-first spool, verification, asynchronous synchronization, and last-known-good service cache |
| explicit transmit/automation authority | RF-safe launch/send capabilities and final preflight enforcement |

The normal route never asks the operator to invent ports, child directories,
settings filenames, database paths, command arguments, working directories, or
dependency order that a qualified recipe can derive.

### GRS-13 Acceptance Matrix And Exit Gate

Implementation is not complete until automated fixtures cross:

- Windows and Linux/Wine;
- existing single-instance, established external multi-instance, and fresh
  station inventories;
- create distinct sibling, adopt unchanged, manual/remote, replace, and
  explicit shared-service operations;
- two radios in the same operating group and two radios in different groups;
- local storage, NAS online, NAS offline during preparation, and NAS loss during
  service; and
- final Save, cancel, injected native failure, injected database failure,
  restart/reload, display rename, and exact launch round-trip.

The integrated gate requires all of the following:

1. Existing native bundles and files remain byte-for-byte unchanged unless the
   final reviewed plan explicitly names a qualified mutation. New siblings use
   the established native parent/convention without reusing any exclusive
   child resource.
2. No native application or operator-content path is created below the FIO
   private configuration root. Existing standard/native paths and intentional
   operator/NAS choices survive prepare, draft save, final save, reload, and
   launch.
3. Receiver/SDR++, JS8Call, FLRig, FLDigi, FLMsg, FLAmp, VarAC/VARA,
   FIO Spotter, external JS8Spotter, and CommStat
   each round-trip their complete canonical identities through every required
   projection. No draft key, stale path, loose command, or adjacent-radio value
   appears after save.
4. Two concurrent radio instances use distinct qualified profiles, endpoints,
   processes, message sources, and launch components. Shared binaries do not
   collapse them. Explicit station services launch once and retain all intended
   radio and access-policy bindings.
5. FLMsg launches with only its supported radio-specific `--flmsg-dir`
   identity while FLDigi owns the matching `--flmsg-dir`/`--auto-dir` handoff.
   FLAmp launches with its radio-scoped `--config-dir` and exact distinct
   ARQ/XML-RPC pairing. A second radio's process satisfies neither identity;
   legacy incomplete rows remain visible for reviewed replacement.
6. Messages retain radio, instance, and receipt-context provenance;
   unsafe/partial files are not presented as complete; duplicate content is
   idempotent without erasing distinct receipts; no source file is moved or
   deleted.
7. FIO Spotter FLAmp Q and FIO BBS are station-scoped and serve only explicit,
   validated canonical publication entries. Cross-group access follows the
   item's ACL rather than service ownership; unauthorized group/callsign/role
   subjects cannot retrieve it. One station responder answers, unpublish is
   atomic/auditable, and failed refresh or offline NAS retains a correctly
   reported last-known-good local service state.
8. Mutable application runtime stays local unless an exact network recipe is
   qualified. NAS synchronization is asynchronous, bounded, restart-safe, and
   cannot block the GUI or unrelated radio lanes.
9. Radio rename preserves stable IDs, paths, ports, and launch commands. Cancel
   and every injected failure preserve prior database projections, native
   files, launch bundles, and service indexes exactly.
10. Exact structured commands containing spaces, backslashes, selectors, and
    platform paths survive persistence and are delivered byte-for-byte to the
    process runner with `shell=False` or platform-equivalent safe execution.
11. Production-shaped copied databases migrate additively and reproject only
    through an explicit migration/reconciliation plan. The source copy remains
    unchanged and any mismatch is reported rather than guessed.
12. Discovery, hashing, directory classification, NAS checks, projection
    verification, and reconciliation run on bounded workers. Add Radio and
    Software Administration remain responsive and use one generation-fenced
    result without duplicate scans.

The implementation slice must update this specification and the work log with
schema/migration decisions, exact model ownership, commands/counts, and any
still-open live Windows, Linux/Wine, external-application, NAS, or radio
qualification. No implementation claim may close this gate using UI-only or
helper-only tests.

Specification work-package ownership:

- primary `gpt-6-astra`, high reasoning: product/storage architecture,
  persistence authority, safety, integration, specification edit, and final
  review;
- `gpt-5.6-terra`, medium reasoning: bounded persistence and cross-spec
  consistency audit; and
- `gpt-5.6-luna`, medium reasoning: bounded acceptance-matrix, edge-case, and
  follow-up publication-scope audits.

GRS-13.1 implementation uses additive `radio_software_identity_sets` and
`radio_software_identity_records` tables. There is no destructive migration and
no automatic backfill of legacy stations. Add/Edit Radio and the authoritative
Software Administration instance assistant capture an expected generation,
write the complete selected identity set inside the existing guided transaction,
and read back exact fingerprints. Settings reload compares canonical records to
persisted application/manifest/launch projections by owning family and radio,
never by searching unrelated rows for matching text. Application configuration,
data, and message fields are compared only with their exact linked fields;
launch executable, `argv[]`, working directory, environment, dependency order,
startup/monitor policy, and readiness evidence are compared with the exact
component launch row. Projection drift is shown as
`Needs attention`. Derivable legacy projection loss is recovered from the same
radio's canonical record and manifest before planning; other projection drift
remains a review warning and does not discard an independently valid saved
recipe. Only a genuine launch-time safety conflict rejects scoped manual launch
or skips that radio during station startup, leaving unrelated launch lanes
available.
Legacy compact fields cannot patch a canonical record independently.

Automated GRS-13.1 coverage includes one production-shaped linked-row
save/reload gate for receiver/SDR++, JS8Call, every selected Fast Light
component, VarAC/VARA, FIO Spotter, external JS8Spotter, and CommStat. That gate
persists application, manifest, exact structured launch, built-in component,
station-process reference, and radio-binding projections, reloads the canonical
records, and requires a clean projection validation. Additional coverage proves
cancel/no-save, stale-generation rollback, explicit rather than JS8-inferred
station-service selection, built-in launch handling, cross-family dependency
handling, cross-radio owner/binding rejection, read-only validation, malformed
or duplicate identity rejection, two-radio CommStat bindings, exact projection
drift, scoped launch blocking, and canonical-editor read-only behavior. The
complete GRS-13 exit gate remains open for native-directory/message-library/
publication/NAS implementation and live external qualification.

GRS-13.3 closes the JS8Call draft-identity leakage found by operator testing.
Its acceptance gate must start with the real distinct-draft allocator, run the
production JS8Call recipe resolver, persist the application/manifest/canonical
projections, reload Software Administration, and build the launch projection.
The gate requires one durable application/canonical key, an operator-readable
radio-derived `--rig-name`, platform-native Qt settings and application-data
paths, and no draft key or private FIO managed-instance root in any persisted
JS8Call path or normal UI text. Helper-only fixtures with hand-authored final
paths do not satisfy this gate. Legacy draft-derived rows remain unchanged and
must exercise the explicit `Needs attention` recovery route.

## GRS-14 — VarAC Topology Recovery And Writer Routing

Operator testing found a state-machine contradiction in the Tri-Mode Add Radio
route: automatic preparation submitted the safe `Standalone` default to the
cluster-only native writer; the resulting `Choose Create cluster or Join
cluster` block then collapsed the VarAC card and hid the arrangement selector.
The operator was told how to recover but was not given the control needed to do
so.

The corrected route has one topology-to-writer contract:

- `Standalone` prepares a distinct, non-mutating VarAC identity and retains
  discovered node-local evidence without starting the cluster writer. It is a
  `Ready with warning` plan because native standalone files remain
  operator-owned; the operator may continue.
- `Create cluster` and `Join cluster` alone start the qualified native cluster
  preparation worker. Next and Save remain disabled until its current
  generation returns a writer-qualified ready bundle.
- A blocked Create/Join result displays its exact reason and keeps the
  arrangement selector visible and enabled. Changing topology invalidates the
  prior context and schedules one generation-fenced replacement preparation.
- The arrangement selector also stays available on the standalone warning so
  the operator may opt into cluster management after automatic discovery. It
  is not hidden in Details or Software Administration.
- Final Review may call a standalone identity prepared only to the documented
  warning level; it may not describe it as a qualified native cluster bundle.
  Conversely, no Create/Join plan may pass Next or Save under the standalone
  exception.

Acceptance requires a real Tri-Mode widget route proving: standalone does not
invoke the cluster worker, the selector remains actionable, Continue is
available for the non-blocking standalone plan, Create/Join still invokes the
worker, a blocked native result keeps the selector visible, and a qualified
result alone unlocks the cluster route. No schema or data migration is part of
this correction.

### GRS-14.1 — Existing-Node Identity, Complete Projection, And Responsive Discovery

Operator retest of the binding route `Settings > Radios > Add Radio > Tri-Mode
> VarAC > Create cluster` exposed a second contradiction after canonical
Add Radio/Software Administration parity was introduced. The arrangement list
could offer **Create cluster** while its opaque selection had no existing-node
identity. Native preparation then reported that the standalone node was missing
or ambiguous and told the operator to “refresh discovery,” although that route
contained no refresh action. At the same time, the already saved VarAC paths
were not projected into Software Administration. The attached CPU evidence
also showed a background JS8 profile phase spending multiple seconds parsing
one settings file while newer generations waited or timed out.

Topology identity and application qualification are separate facts:

- a durable `device_profiles.varac_node_id` link identifies an existing VarAC
  node for topology even when one or more application paths still need review;
- path completeness determines whether the native writer is qualified, not
  whether the linked node exists;
- when there is exactly one linked standalone node and no cluster, both the
  arrangement metadata and the native-preparation boundary must resolve that
  same node deterministically. The worker may recover a missing UI metadata ID
  from that unique durable topology snapshot; it must not guess when there are
  zero or multiple eligible standalone nodes;
- an explicit positive node ID that no longer exists is stale evidence and
  must fail without substituting another node;
- ambiguous stations must present the actual standalone-node choices in the
  arrangement control. A recovery message may name only an action that is
  visible on the current route; it may not instruct the operator to use a
  nonexistent refresh control.

The VarAC path projection has one lossless contract. The selected existing
node contributes its installation, INI, database, VARA runtime/INI, launch, and
incoming facts; its linked radio contributes outbox, BBS, and archive facts.
The prepared new member contributes its generated install-adjacent INI,
distinct VARA runtime, node-local incoming/outbox, shared database/BBS/archive,
working directory, ports, and structured launch vector. Add Radio Connections,
Review, the retained canonical draft, saved application/manifest/profile rows,
Launch Control, and Software Administration must display the same nonblank
values after preparation and after reload. A field may be blank only when no
reviewed or safely derived fact exists, in which case it is a warning or
operator correction—not silently discarded topology.

Discovery remains background-only and generation-fenced, but background work
must also be bounded. JS8 settings readers must read no more than a documented
maximum candidate size, check cancellation between candidates, and skip an
oversized or unreadable candidate as diagnostic evidence. A timed-out phase is
single-flight as required by GRS-12.2; newer generations reuse it rather than
starting another parser. UI publication consumes one immutable result and does
not synchronously repeat the full Software/Review rebuild for the same state.

Acceptance requires production-shaped, real-store tests rather than only
hand-authored final mappings. The fixture must contain one linked standalone
VarAC node and no cluster, pass through inventory classification and topology
recommendation, intentionally omit the UI-carried node ID at the native
boundary, and still prepare the same unique node with populated canonical
paths. Companion cases must prove that a stale positive ID and multiple
standalone nodes remain blocked. Persistence coverage must reload the linked
radio and Software Administration projection and compare every VarAC path and
launch fact. Discovery coverage must prove normal JS8 settings still parse,
oversized candidates are bounded/skipped, cancellation stops between
candidates, and repeated generations do not multiply identical work. The
focused VarAC/discovery/UI route, broader guided software and Software
Administration parity suites, changed-file compilation, and `git diff --check`
must pass. Live Linux/Wine completion remains the external qualification gate.

### GRS-14.2 — Collision-Free Managed VARA Runtime Allocation

Production retry testing reached the qualified native writer but failed with
`Managed VARA target runtime must be absent`. The writer was correctly refusing
to replace an existing runtime; the preparation layer was incorrectly proposing
the same fixed `varac-native/VARA` target on every attempt. A prior interrupted
attempt, an already reviewed FIO runtime, a duplicate human-readable radio
label, or a broken symbolic link could therefore make an otherwise safe Add
Radio plan permanently non-actionable.

The writer's no-replacement rule remains absolute. Guided preparation owns the
collision-free name choice before it constructs the immutable plan:

- on Linux/Wine with proven drive evidence, each VARA runtime created for a
  converted standalone member or a new cluster member starts with the readable
  preferred name `VARA-<radio-slug>` at the Wine drive root so VarAC receives a
  native drive-letter path; qualified layouts without that evidence retain the
  readable preferred name `VARA` below the member's radio-scoped
  `varac-native` directory;
- any existing filesystem object at that path—including a directory, file, or
  broken symbolic link—counts as occupied and is never removed, entered,
  copied over, or treated as proof of FIO ownership;
- preparation chooses the first unused sibling `VARA-2`, `VARA-3`, and so on,
  and also reserves targets already assigned to another member in the same
  plan. Two members whose display labels normalize to the same directory must
  still receive distinct targets;
- read-only re-preparation against unchanged filesystem evidence returns the
  same target. If the selected target appears after review, final apply retains
  its independent revalidation and stops before mutation; the operator can
  prepare again to receive the next unused target;
- the selected runtime, its `VARA.ini`, derived executable path, launch vector,
  manifest, Add Radio review, saved application/profile data, Launch Control,
  and Software Administration all use that one canonical selected path. No
  later layer may reconstruct the unsuffixed default.

This recovery is automatic and is not an operator path-selection question.
The UI may explain that an occupied location was preserved and a numbered
runtime was selected, but it must not present the writer's internal safety
exception as a dead-end correction task. Direct writer requests that name an
existing target remain blocked, preserving the defense against destructive
replacement.

Acceptance requires preparation tests for an occupied preferred runtime,
repeat preparation stability, same-label members, and a broken symlink; the
tests must prove the occupied object is unchanged and that preparation creates
nothing. Existing native-writer tests must continue to prove that an explicit
existing target is rejected. Focused native preparation/writer tests, the full
guided VarAC route, final-apply/persistence tests, changed-file compilation, and
diff hygiene must pass. No schema or production-data migration is part of this
correction.

### GRS-14.3 — VarAC Cluster Ownership And Atomic Standalone Conversion

Status: automated implementation gate passed 2026-09-20; supported-platform
operator qualification remains open.

Creating a cluster from an existing standalone node changes the ownership of
some reviewed resources; it does not create a second unrelated installation.
The persistence boundary, Add Radio Review, and Software Administration use
one exact ownership model:

- the VarAC executable/installation and its launch working directory are
  reusable application resources, not member-exclusive directories;
- the selected cluster database and cluster BBS/archive directories are owned
  by the cluster and are nonexclusive in each member manifest;
- each member's VarAC INI, cloned VARA runtime and VARA INI, incoming folder,
  outbox, endpoint ports, and positive cluster member number remain exclusive;
- unknown future claims retain their reviewed exclusivity; normalization may
  weaken only the named cluster-shared kinds above; and
- a standalone manifest that previously described a now-shared resource as
  exclusive is re-scoped in the same database transaction as cluster creation,
  both memberships, the new member manifest, and the radio assignments.

When the qualified native writer changes the existing member's INI, VARA
runtime, or launch vector, FIO updates the existing application row and its
canonical Software Administration identity in that same outer reviewed save.
Unrelated software identities on the existing radio are preserved. A failure
in any application, manifest, membership, canonical-identity, or launch
projection rolls back the complete FIO transaction and triggers compensation
of the already-applied native session.

Persistence validation occurs before generic conflict reporting whenever the
authoritative cluster membership can name the problem. A duplicate member
number is reported as that operator-correctable choice. A genuinely duplicated
member INI, VARA runtime/INI, mailbox, or endpoint remains a blocking collision;
shared application/cluster paths never are. The failure dialog must say
`Nothing Saved`, not `Saved`, and the log records family, radio, persistence
phase, exact rejection detail, scheduled native compensation, and compensation
completion or recovery failure.

Acceptance requires a production-shaped standalone-to-cluster store test with
two radios. It must begin with the old standalone manifest claiming the common
executable, working directory, database, BBS, and archive exclusively, then
prove that final adoption succeeds and reloads both member manifests with the
shared/member split above. It must also prove that duplicate member resources
and member numbers still fail without partial rows. Focused assistant payload,
canonical identity, VarAC arrangement, manifest persistence, native
transaction, changed-file compilation, and diff-hygiene gates must pass. No
schema or destructive data migration is part of this correction.

### GRS-14.4 — Interrupted Cluster-Creation Resume

An earlier interrupted or pre-GRS-13.5 save may leave the reviewed cluster and
its first radio durable while the new radio was not committed. That is a
recoverable topology, not evidence that the operator should create a duplicate
cluster or understand the partial database state.

When one existing enabled cluster has exactly one saved radio member and its
public identity is the deterministic identity FIO would derive for that member
plus the new radio label, Add Radio presents an explicit
`Resume <cluster>: add <new radio> as member <n> — Recommended` choice. It uses
the cluster's next available positive member number and the normal Join writer
route. The selector remains unselected until the operator confirms the resume;
FIO does not silently change topology. The ordinary standalone and distinct
new-cluster alternatives remain available and clearly named.

Matching uses durable cluster membership and radio identity, never orphan
manifests, directory-name similarity alone, or an unlinked diagnostic node.
Zero matches or multiple matches receive no resume recommendation. Resuming
does not delete, rename, or replace an existing application file or FIO record;
the final Add Radio transaction adds the reviewed member and all selected
software identities under GRS-13.5.

A manifest whose named application row no longer exists is retained as
diagnostic evidence but has no live resource owner. It may not reserve an
endpoint or exclusive path against a reviewed recovery transaction. Valid
manifests linked to existing application rows retain all normal collision
protection. Recovery does not delete the orphan record automatically.

Acceptance requires snapshot and real-widget regressions for a production-
shaped `FTDX-10 + FT-710 VarAC` cluster containing only FTDX-10. They must prove
that FT-710 is offered as member 2 through an explicit recommended resume
choice and that unrelated existing clusters retain the ordinary Join wording.
Companion store coverage must prove that a retained orphan manifest cannot
block the replacement application's reviewed endpoint or path while the orphan
remains available for diagnostics.
