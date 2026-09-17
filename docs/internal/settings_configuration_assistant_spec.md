# Settings Configuration Assistant Spec

Status: software-centered administration implementation complete for multi-rig
2.0.0 private testing. SCA-S0 through SCA-S4 have passed their automated exit
gates. Linux desktop qualification remains an operator-assisted production
check and is recorded below rather than treated as automated evidence.

The software-centered administration work below is the current implementation
authority for Settings software workflows. It refines Phases 2 and 3 without
moving operational FIO Spotter or BBS administration back under radio Settings.
The guided multi-instance lifecycle, manifest, collision, launch, and Cluster
VarAC contract is defined in `multi_instance_software_administration_spec.md`.
The unified radio-first workflow, including observer Fast Light, Receiver Guard,
Receive Schedule, native-writer qualification, and Add Radio handoff into this
workspace, is defined by `guided_radio_software_configuration_spec.md`.

## Goal

FreqInOut settings should feel like an operator assistant, not a raw collection
of expert-only fields. FIO should understand radios, app profiles, ports,
launchers, paths, shared tools, operating groups, and RF risk well enough to
guide the user toward a stable setup while preserving operator control.

## Principles

- Inspect existing configuration before proposing new profile, port, path, or
  launcher values.
- Separate shared definitions from per-radio assignment.
- Put technical endpoint/path validation in configuration guidance and health,
  not in every day-to-day assignment workflow.
- Warn clearly about conflicts and RF Guard risks, but let the operator make
  the final decision after acknowledgement.
- Make radio identity and app instance identity unmistakable in multi-rig
  screens.
- Keep developer/support details available, but out of the normal operator
  path.

## Software-Centered Administration

### Operator mental model

The normal workflow is:

`Choose software -> see radios using it -> choose a radio -> choose a task -> configure`

Settings navigation exposes `Main`, `Radios`, and `Software`. `Radios` owns
radio identity, hardware/control backend, activation, plan/schedule assignment,
RF Guard, and a concise software-assignment summary. `Software` owns detailed
application administration. A software chip on a radio deep-links to the same
Software workspace; detailed editors are not duplicated on the Radios page.

The Software workspace must:

- show software-family cards before presenting detailed fields;
- show an `All` summary plus a chip for every radio using the selected family;
- show disabled, unassigned, missing, shared, and needs-attention states without
  relying on color alone;
- keep a persistent identity banner such as
  `Editing JS8Call for FIO-A - Instance: FIO-A JS8` above the editor;
- disclose every affected radio before editing a shared instance;
- segment configuration by operator task rather than present one long table or
  form; and
- label saves with the exact scope, for example
  `Save JS8Call for FIO-A`.

Selecting a family, radio, or task is navigation only. It must never save,
discard, scan the filesystem, inspect processes, probe an endpoint, or perform
radio I/O. Unsaved drafts remain keyed by owning radio and software instance.
Switching context preserves drafts and marks affected chips. A deliberate
secondary Save All action may exist, but a generic save must not silently commit
unrelated radio drafts.

### Software ownership and task taxonomy

- **JS8Call** is a radio-assigned application instance. Tasks are Overview,
  Application & Profile, API & Radio, Message Storage, Ingest & Forms, Launch,
  Health, and Advanced.
- **Fast Light** is a family. FLRig and FLDigi endpoints are radio-assigned.
  FLMsg and FLAmp are shared station applications by default, while the workspace
  still shows which radios use their message and transfer workflows. Tasks are
  Overview, FLRig Control, FLDigi Modem & Logs, FLMsg, FLAmp & Signing, Message
  Folders, Launch, Health, and Advanced.
- **VarAC** is a radio-assigned application instance. Tasks are Overview,
  Application & Radio, Runtime & Paths, Inbox & Outbox, Inbound Guard, Cluster,
  Launch, Health, and Advanced. Shared BBS administration remains in the
  top-level BBS service; Settings provides a contextual link only.
- **CommStat** is a standalone external tool using an explicit JS8/radio
  transport mapping. Its installation, launch, health, and mapping are not
  presented as JS8Call settings.
- **External Spotter** is an optional standalone application. It appears only
  when configured or discovered and owns its installation, launch, profile, and
  form-synchronization guidance.
- **FIO Spotter** is a built-in FIO service. Settings shows dependency and radio
  mapping status and links to the top-level FIO Spotter workspace for rules,
  Expect administration, watches, forms, and activity.
- **Launch Control** is a cross-cutting task and may summarize all configured
  applications, but it does not replace each application's scoped Launch task.

### Read model and performance contract

The workspace renders from a bounded, immutable software-administration
snapshot derived from device-profile assignments, linked JS8Call/Fast Light/
VarAC rows, and cached readiness evidence. The snapshot supplies the reverse
index from software instances to radios and identifies unassigned instances.
The initial read model is additive and requires no schema migration.

Opening Settings, choosing a family, changing a radio chip, switching a task,
resizing, changing theme, and painting are cache-only operations. Discovery and
validation run only on explicit request or a bounded background refresh. They
are asynchronous, coalesced, cancellable, generation-checked, and publish an
immutable result. The prior known state remains usable while refresh runs. One
slow or unavailable application endpoint must not block another editor or the UI.

### Responsive and accessibility contract

At wide sizes, family cards, radio chips, task navigation, and the editor may use
multiple columns. At 1000x700, 900x560, and Large Text, controls wrap or stack
without horizontal page scrolling and the editor remains the primary work
surface. Chip text includes identity and state, has an accessible name and
tooltip, and never communicates readiness by color alone. Hints follow the
project-wide neutral-identity rule.

### Delivery slices and exit gates

1. **SCA-S0 - specification and read model.** Add the immutable reverse-index
   model and pure tests for assigned, shared, disabled, and unassigned instances.
   Exit: no migration or runtime writes; focused model tests pass.
2. **SCA-S1 - workspace shell.** Add `Settings -> Software`, family cards, radio
   chips, identity banner, task routing, and radio-page deep links. Existing
   editors may be reused behind the shell. Exit: the complete software-first
   navigation path works without I/O and responsive/theme tests pass.
3. **SCA-S2 - task-oriented editors.** Separate JS8Call from FIO Spotter,
   External Spotter, and CommStat; segment Fast Light and VarAC by operator task.
   Exit: ownership is unambiguous and existing values round-trip unchanged.
4. **SCA-S3 - scoped drafts and saves.** Add exact save scope, dirty-state chips,
   shared-instance warnings, and deliberate Save All behavior while retaining
   cross-radio rejection and the single-active-radio legacy projection rule.
   Exit: wrong-radio and shared-instance tests pass.
5. **SCA-S4 - guided discovery and qualification.** Route existing path/endpoint
   discovery through bounded background work, finish help and accessibility,
   and qualify light/dark, Normal/Large Text, Linux, 1920x1080, 1000x700, and
   900x560. Exit: focused and integration suites pass; any platform-assisted
   evidence is recorded rather than silently waived.

### Implemented through SCA-S2

The Software workspace now hosts declarative, task-oriented editors over the
existing per-radio software state. JS8Call no longer presents CommStat,
External Spotter, or legacy Expect administration as JS8Call settings. CommStat
owns its launcher and explicit JS8 transport mapping; External Spotter owns its
optional launcher/import workflow; built-in FIO Spotter exposes dependency and
radio mapping plus a route to its top-level operational workspace. Fast Light
and VarAC are segmented by the task taxonomy above. The former monolithic
editors remain non-navigable compatibility adapters until their state-loader
dependencies are retired; they are not a competing operator surface.

Task navigation is cache-only. Existing per-radio values populate the editor
without changing their database keys or schema, and dotted message-folder
values retain their nested structure. The hidden legacy Spotter and Expect
tables are no longer synchronously queried and populated during Settings
startup. Explicit import and operational management remain available from the
owning product surface.

### Implemented in SCA-S3

Each editable family now has an explicit field partition and an exact-scope
save label. A selected-family save merges only that family's draft fields into
the selected radio's persisted base, including individually owned nested
message-folder entries. Other software values and other dirty family drafts are
preserved. The existing source-radio identity rejection remains authoritative,
shared instances name the other affected radios before confirmation, and the
legacy single-active-radio projection runs only after a successful scoped save.

Dirty state is keyed by `(radio, software family)` and is communicated in family
and radio chip text, accessible names, the identity banner, and the selected
editor. `Save All Changes` is deliberate, secondary, disabled when no drafts
exist, and retains dirty indicators after failure. The global `Save Settings`
action explicitly excludes Software workspace drafts. Default endpoint values
alone no longer create unrelated JS8Call or Fast Light instance records while
saving another family.

### Implemented in SCA-S4

Path discovery is an explicit `Find installed software` action in the selected
task editor. It captures a copy of already-loaded Settings values on the UI
thread and performs filesystem discovery in one Settings-owned worker lane.
Repeated requests are coalesced to the newest request, the active worker
receives a cancellation request, and generation plus family/radio/task identity
guards prevent a stale result from filling a different editor. Discovery fills
blank fields only and reports how many values were filled, preserved, or not
found. Existing operator values are never silently replaced.

Worker completion, cancellation, and failure all stop the worker thread. Normal
navigation never starts discovery. Shutdown requests cancellation and waits no
more than 1.2 seconds; an unusually slow filesystem operation is retained until
its thread exits instead of blocking shutdown or allowing a running `QThread`
to be destroyed. Endpoint health checks remain explicit and use the existing
asynchronous status service.

At constrained height, explanatory prompts collapse while the software,
radio, and task chip strips remain available and the editor becomes the primary
surface. Light and dark themes, Normal and Large Text, 1920x1080, 1000x700, and
900x560 are covered by offscreen layout/repaint tests. Software controls and
status surfaces have accessible names and communicate state in text rather than
color alone. The operator guide now documents the software-first workflow,
ownership boundaries, exact-scope save behavior, explicit discovery, and the
top-level FIO Spotter and BBS routes.

Automated acceptance evidence: 246 focused and adjacent Settings/status tests
pass with 23 intentional environment skips; the stricter SCA-only combined gate
passes 213 tests. Python compilation and `git diff --check` pass. A 900x560
render was visually reviewed with all three chip selectors, the identity banner,
all API fields, and the exact-scope editor actions available. A repository-wide
run was intentionally stopped after unrelated legacy tests accumulated many
scheduler executor threads and stalled in a theme-heavy Inbox test; interrupting
that non-gating run triggered its existing Qt/process teardown fault. No SCA test
failed, and no schema migration or destructive data operation was introduced.

Linux production qualification is operator-assisted because this macOS worktree
cannot certify a real Linux window manager, installed application paths, or
desktop accessibility stack. The production check is: open Settings > Software,
exercise each chip strip at the three supported sizes and both text sizes, run
one explicit discovery, switch context before it completes, then close FIO while
a discovery is active. The UI must remain responsive, stale results must not
move to the new editor, and shutdown must produce no running-thread warning.

### Production layout correction and permanent presentation contract

Production screenshots exposed a stale-height failure that isolated widget
tests did not reproduce. The Settings section stack had been fixed to the
placeholder page's early `sizeHint`; software task editors are created after
that point, so their content and footer actions were clipped even at 1920x1080.
The Software page must instead track the current Settings viewport. It must not
derive its height from either the initial placeholder or the largest hidden
legacy page, and it must not create page-level horizontal or multi-screen
vertical overflow. A resize, family change, radio change, or task change must
leave the active editor and its bottom action row reachable.

`All` is a read-only family overview, not an implicit editable radio. It shows
the software-to-radio assignments, instance names, cached status, shared use,
and unassigned instances, then directs the operator to choose one radio.
Radio-owned task forms, Browse controls, health checks, and exact save actions
must never appear active in the All context. The task strip is hidden there to
avoid implying that blank aggregate fields can be edited. Selecting one radio
restores the complete task strip and exact radio editor.

The embedded workspace has one visible Software Administration heading. At
constrained height it removes duplicated status and secondary assignment text,
while preserving software and radio selection, the selected-context banner,
task selection for a chosen radio, complete form controls, explicit operational
actions, and the exact-scope save. Editor footers use one row when width allows
and wrap at compact width. Empty and read-only tasks do not show meaningless
save controls or consume the editor with an empty form scroller.

No-field and action-only tasks must also clear the hidden form scroller's
stretch allocation. Their heading, explanation, current status, and action
form one compact top-aligned card with ordinary spacing; unused room remains
below the card. This rule applies consistently to Overview, Health,
cluster/route, import, and synchronization tasks in every software family.

The neutral state for an assigned instance with no current readiness evidence
is **Not yet verified**. It means FIO has not run or received a current check
for that software instance; it is not a failure, offline result, or setup
warning. The radio chip tooltip directs the operator to select the radio and
open Health. User-facing Software Administration must not use the ambiguous
`Not checked` wording.

Software-instance creation is radio-first. A radio can use JS8Call, Fast Light,
and VarAC together, but it can have only one assigned instance in each family,
and one runtime instance cannot be shared by independently controlled radios.
The family, radio, and task chip rows are exclusive navigation groups: one
choice is visibly active at each applicable level, including after a repeated
click. If no radio exists, the instance workflow stops at `Create a radio
first`; it never creates an operational orphan.

The Settings > Radios > Add Radio navigator has a stable seven-position
contract: `Radio -> Operating Model -> Software -> Connection -> RF Guard ->
Schedule -> Review`. All seven numbered controls remain visible from first
render through Review. Applicability may change as the operator chooses a radio
role, setup type, software, or control route, but that must not remove a numbered
control or leave gaps such as `1, 3, 5`. A non-applicable step remains visible,
disabled, and explicitly identified by its tooltip/accessibility description as
not required for the current setup. Back, Next, direct-step eligibility, and
save gating traverse only applicable steps. In particular, a receive-only SDR
uses Operating Model and Connection while RF Guard and Schedule remain visible
as not applicable; a conventional radio also uses Operating Model and keeps
Connection visible even before a software/control choice makes endpoint fields
necessary. Step 2 presents enabled shared models for a conventional radio and
only enabled receive-only models for an observer/SDR. Editing a radio preselects
its current assignment. Saving requires a real persisted selection, saves a new
radio inactive, assigns that selected model, and only then activates a first
transceiver or first/only observer. An assignment failure leaves the new radio
inactive and recoverable rather than silently applying a default. Every step
control uses a font-derived height and the complete strip wraps without clipping
at the supported compact size.

The model inventory is self-healing at this boundary: protected default and
receive-only models must exist and be enabled before the selector is populated,
including when an early development database retained a disabled protected row.
The repair must update the protected row in place and must not duplicate models,
create a radio, or change runtime-primary selection.

Application and JS8Call-profile discovery is operator-triggered, bounded
background work. Opening the dialog, choosing a setup type, changing steps,
painting, and resizing are cache-only. `Configure Automatically` disables only
its own action while a worker searches; the rest of the dialog remains usable
and existing field values are never overwritten. No global event-loop pump may
run deferred Settings, Ops Center, mesh, or scheduler timers during main-window
construction merely to repaint startup progress.

The embedded instance assistant must visibly present its complete guided path:
`Purpose -> Find or create -> Identity -> Connections -> Files -> Launch ->
Review`. The current step is themed and selected, prior steps remain available,
and only the next eligible step is enabled so a required radio or replacement
decision cannot be skipped. Step controls use font-derived heights and wrap as
a compact grid at constrained width. When the caller supplies a radio context,
that radio is selected before the first render and `Next` reflects it
immediately. If the inventory contains exactly one valid radio, the assistant
selects it automatically; multiple radios require an explicit operator choice.

For an occupied family slot, the guided path offers an explicit reviewed
replacement rather than requiring manual disassociation. Current and proposed
identities remain visible through Review, and the persistence service uses the
expected current ID so a stale or failed replacement cannot disturb the working
assignment. `Assign Existing Instance` is a recovery path limited to compatible
unassigned records. `Disassociate` is an Advanced, confirmed action that removes
only FIO's family link, family use flags, managed startup links, and applicable
VarAC membership. It retains the external application, configuration, storage,
messages, and files.

Permanent matrix coverage includes every declared family and task, selected
radio plus All, light and dark themes, Normal and Large Text, and 1920x1080,
1000x700, and 900x560. The real SettingsTab integration must additionally prove
that the stack follows the viewport, has no outer scroll at the supported
compact size, retains a usable editor, and keeps the footer reachable.

Regression correction: section-navigation controls must only be constructed
when they have an owning layout and parent. Software uses the top-level Settings
navigation plus its embedded family/radio/task chips; it must not create a
second section-nav button. A parentless Qt button becomes a top-level window and
can cover the application when visibility is refreshed. The integration gate
opens Software and clicks each navigation tier while asserting that no new
top-level window appears and the workspace remains embedded in Settings.

Production family-selection correction: the active Software workspace must not
derive its hard height from `QScrollArea.viewport()`. That viewport is partly
child-driven and can briefly report zero or a stale size while the deferred
Settings load, a software-family selection, and Qt layout activation overlap.
The shared section stack instead uses the already allocated outer scroll-area
height with a small nonzero floor. The active Software page and stack receive
the same stable bound, and the stack returns to a non-expanding policy so a
large hidden legacy Settings page cannot take over the workspace. A subsequent
real resize recalculates the bound from the new outer allocation.

The permanent lifecycle gate must open Settings > Software, select JS8Call,
rebuild the same cached snapshot as the deferred initial load would, and repeat
family/radio/task selections across 1920x1080, 1000x700, and 900x560. At every
settled event-loop boundary the Software section and selected editor remain
visible, the shared stack stays between its usable floor and the containing
screen, the outer Settings scrollbars remain inactive, and no top-level window
is created. Navigation, resizing, snapshot rebuilding, and repainting remain
free of database, filesystem, process, socket, and radio work.

An open `Create or use instance` or `Replace instance` assistant is an explicit
in-progress operator workflow. It remains the current embedded editor through
Settings activation, cached-snapshot rebuilding, health/status refresh, and
normal host editor reconstruction. Those passive updates may refresh the hidden
summary or task editor for later use, but they must neither hide the assistant
nor replace its status with an interaction warning. Only Cancel, a successful
reviewed save, or the explicit `Create a radio first` route closes the assistant.
The lifecycle gate covers both the All-radio create path and selected-radio
replacement path, verifies that the assistant never becomes a top-level window,
and verifies that Cancel reveals the newly refreshed summary or task editor.

## Phase 1: Settings IA Cleanup

### Condition Alerts

Problem: the current all-fields table is too wide to use. Operators need to
scan rule identity, enabled state, source, match intent, level, and action,
then inspect details for sender/auth/pattern specifics.

Target behavior:

- Use a compact rule list with only high-signal columns visible.
- Keep advanced fields in the selected-rule detail area.
- Preserve existing saved rule format.
- Keep template reset, add, delete, and save actions available.
- Make auto-SOP behavior clearly separate from rule enablement.

Initial implementation: Condition Alerts now uses a compact visible rule list
and a selected-rule detail panel for sender policy, auth, targets, pattern, SOP
level, and action. The saved settings schema is unchanged.

### Operating Models

Problem: Operating Models are shared definitions, but their current placement
under Radios implies that they are radio-local.

Target behavior:

- Move Operating Model configuration/admin to `Settings -> Main`.
- Keep Operating Model Assignment under `Settings -> Radios`.
- Avoid modal-heavy editing in a later phase by converting model editing into a
  full-width editor surface.
- Explain that radios assign shared models.

Implementation note: this remains a staged IA migration because it touches
shared model editing, assignment wiring, and existing guided setup. Do not move
only the button or label without moving the editor surface and tests together.

### Schedule Assignment

Problem: endpoint details are overbaked in schedule assignment. By this point
the user should be choosing which plan a radio follows, not auditing control
ports.

Target behavior:

- Hide endpoint columns from the normal Schedule Assignment table.
- Keep RF Guard status visible.
- Move endpoint/path concerns to readiness, health, and configuration guidance.

Initial implementation: Schedule Assignment now uses a compact 7-column table
with radio, plan, state, and RF Guard status visible. Endpoint details are not
shown in the normal assignment table.

## Phase 2: JS8Call, Spotter, And CommStat Setup

JS8Call is one of the most important digital tools in the FIO workflow. The
configuration model must distinguish JS8Call, FIO Spotter, External Spotter,
and CommStat as separate entities.

Target behavior:

- Provide a guided JS8Call instance/profile creation workflow.
- Inspect existing JS8Call profiles, save folders, inbox paths, FIO radio
  profiles, API/UDP ports, and launch entries before proposing values.
- Generate non-conflicting profile paths, ports, and launch entries.
- Make the JS8 profile name and assigned radio identity obvious.
- Treat FIO Spotter as the self-aware default when used.
- Bundle FIO-managed Spotter forms and clearly show the managed forms location.
- Show External Spotter only when configured.
- If FIO Spotter and External Spotter both exist, sync/copy forms into the
  FIO-managed location. Hiding forms in FIO should not physically delete source
  files.
- Keep CommStat as a standalone external tool configuration surface.
- Add configuration guidance for JS8 endpoint conflicts. If a shared JS8
  control client, profile, save folder, or traffic folder points at a different
  radio than the selected FIO radio, health/readiness should make that visible
  and route the operator to the exact settings area to fix it.

Linux JS8Call-Improved guidance:

- On Linux with JS8Call-Improved 2.5 or higher, recommend JS8Call CAT/PTT via
  external `rigctld` to FLRig when appropriate.
- Generate a non-conflicting `rigctld` launcher rather than asking the operator
  to hand-write shell scripts.
- Avoid common ports such as 4532 and 4537; inspect configured ports first.
- Present this as a recommended Linux control path, not as operator fault.

## Phase 3: Fast Light Profile Creation

Target behavior:

- Guide creation of radio-aware FLRig and FLDigi profiles, configs, launchers,
  and ports.
- Inspect existing FLRig/FLDigi profiles and FIO radio assignments to prevent
  preference/profile cross-contamination.
- Present FLMsg and FLAmp as shared tools unless an advanced workflow requires
  otherwise.
- Make each FLRig/FLDigi instance's radio identity clear.
- Treat FLRig stability as high priority because it is commonly the bridge
  between software and the physical radio.

## Phase 4: Guided Plan Builder

Target behavior:

- Guide HF Daily and HF Nets creation step by step from an Operating Group.
- Suggest known net resources when available.
- Surface RF Guard conflicts during plan building, especially when adding a
  second radio.
- Allow override after clear warnings and acknowledgement.
- Add documentation that FIO provides guidance only; the operator is
  responsible for final decisions and any equipment risk.

## Phase 5: Compose And Tools

Compose:

- Add JS8Call compose support for addressed messages and standard JS8 traffic.
  Status: implemented for guarded addressed sends and standard JS8 traffic.
  Map handoff, JS8Spotter MCForms send, selected-target clearing, self-send
  prevention, peer-schedule guidance, path guidance, and tune prompting are
  implemented.
- Add CommStat RF-only compose using the current CommStat/SuperSpotter format.
  Status: implemented for RF-short StatRep and brevity-with-comment sends via
  the selected JS8Call radio.
- Include CommStat brevity codes. Status: implemented for validated RF brevity
  code entry; a richer operator catalog can be added later.
- Exclude CommStat internet send functionality.

Tools UI:

- Add a safe operator-facing Tools area for shipped utilities such as launcher
  creation, repair, diagnostics, dependency checks, and folder/log access.
- Keep developer/debug tools hidden unless support mode is enabled.

Linux guided install:

- Later P4 feasibility work. Consider concepts from AmRRON setup scripts, but
  avoid high-risk OS package management unless a narrow safe slice is defined.

## Phase 6: VarAC BBS

Slice 2 of the production reliability remediation supersedes the earlier
interim tab layout in this section. VarAC BBS remains a P2 workstream, but the
station-owned catalog, first-class workspace, retention/source-state model, and
shared Messages checkbox workflow are now implemented. Future assistant work
must build on that ownership boundary rather than moving shared controls back
under a selected radio.

Safer multi-radio BBS model:

- Treat the Managed BBS Library as the shared source of truth. It contains
  reusable BBS locations, helper-file text, retained/copied files, sweeper
  rules, and operator-managed access policy. Internal storage keys may keep the
  historical `vault` name for compatibility, but user-facing UI should say
  `Managed BBS Library`.
- Treat each VarAC radio instance as having a separate live BBS folder. FIO
  publishes or copies the selected library content into that radio-specific live
  folder. Two VarAC instances must not be pointed at one mutable live BBS folder
  unless an expert operator explicitly accepts that risk.
- Teach the model in the UI as:
  `Managed BBS Library -> assigned locations -> FIO-A Live BBS / FIO-B Live BBS`.
  The preview should show both library structure and what the selected radio
  will serve.
- Keep VarAC Multi-Instance Cluster setup tied to radio-specific paths and
  launch. Cluster mode is runtime coordination for distinct VarAC instances; it
  is not required for a single VarAC instance or ordinary BBS monitoring.
- Make scope obvious on every VarAC page. Native launcher, inbox, outbox, and
  `Inbound Guard` remain radio-specific in Settings. Live BBS folders and
  enablement join the shared library, visitor-facing structure, publication
  membership, retention, helpers, and access policy in the direct top-level
  `BBS` service, reachable from Messages `+BBS` and the radio Settings link.
- Rename `VGuard` in operator-facing UI to `BBS Access Guard`. The function is
  inbound file protection based on sender trust; it is separate from the Managed
  BBS Library and from message-signature/hash verification.
- The configured-radio selector above Settings should stay compact enough that
  dense configuration pages remain usable. It should identify the selected radio
  and expose activation/default/app-edit actions without consuming the page.

Target behavior:

- Make VarAC BBS configuration and status clear without mixing it into the
  selected-radio Settings mental model. The radio Settings surface contains
  `Radio Paths` and `Inbound Guard`; `Manage FIO BBS` opens the top-level BBS
  service. Its guided order is `Radio Service`, `Locations & Access`,
  `Publishing`, `Visitor Preview`, and `Visitor Helpers`.
- Provide VarAC Cluster node configuration guidance that explains when cluster
  mode is useful, what each node contributes, and which radio/profile owns each
  VarAC instance. Initial guidance is now present in Settings and should remain
  explicit that single-instance VarAC and normal BBS monitoring do not require
  cluster mode; separate VarAC instances should use distinct paths, ports, and
  folders unless sharing is intentional.
- Keep BBS locations, vault handling, access-code state, import/copy behavior,
  and message surfacing understandable to a normal operator.
- Manage file purge/retention by BBS location, not only globally. Each managed
  location should be able to declare its own age policy and archive behavior.
  Initial implementation captures, previews, and enforces age-based managed
  location archival through the BBS auto-archive pass.
- Treat BBS as a message entity under Messages. Operators should be able to
  browse BBS-relevant content without mentally translating from VarAC internals.
  Initial implementation adds a BBS focus in Messages for live and archived BBS
  file rows.
- Treat BBS file management as FIO-owned once the folders are configured.
  Operators should be able to review, archive, and delete files from the live
  VarAC BBS folder, VarAC incoming folder, VarAC outgoing folder, managed BBS
  location folders, and BBS archive from FIO. File actions must preserve origin
  context, avoid filename collisions, and avoid deleting original FLMsg/FLAmp
  source artifacts when the operator is only removing a copied BBS item.
- Show a preview of the managed BBS structure before writing or publishing it so
  the operator can understand what callers will see. Initial implementation
  previews root helper files, visible location helper files, source files under
  each location, and hidden/disabled location behavior without publishing.
- Preserve radio awareness where it matters, but avoid implying that shared BBS
  artifacts are duplicated per radio unless they truly are.
- Ensure Compose, Messages, Map, and health/status surfaces describe VarAC BBS
  traffic consistently.
- Add a configurable sweeper from VarAC BBS Inbox and FLMsg/FLAmp inputs into
  the Managed BBS. The matching model should support sender/from filters plus
  subject-contains filters and should allow one source rule to copy into
  multiple managed BBS locations. Initial implementation adds the pure sweeper
  rule model, matcher, copy-planning helper, and explicit safe copy helper for
  VarAC BBS, FLMsg, and FLAmp sources. Radio Settings exposes only its adapter;
  station-owned review and publication live in Managed BBS. Background rule
  application remains a future slice.
- In Messages, preserve the existing `+BBS` action and add a clear way to remove
  FLMsg/FLAmp content from BBS sync without deleting the original message or
  source artifact. The implemented checkbox dialog preselects current station
  locations; clearing one or all locations disables those memberships while
  preserving the source artifact.
- `+BBS` applies equally to source-row and projection-first Inbox rendering.
  When more than one eligible live or managed BBS destination exists, selecting
  `+BBS` opens one checkbox list so the operator can publish to multiple
  locations in one action. Managed target IDs/names must survive the UI mapping;
  the action must never silently fall back to a different BBS location.
- Future Settings/Messages work should add age-based archive sweepers for
  original FLMsg and FLAmp receive folders. This is separate from BBS copy
  removal: the user should be able to keep radio-message archives clean without
  confusing that with deleting or unpublishing BBS copies.
- Treat BBS repair and diagnostics as candidates for the future Tools UI.

## Permanent Nested-Workspace Stability Gate

Settings is a deferred-load surface embedded in MainWindow. Every Settings
section stack, Software Administration editor stack, and guided-assistant step
stack must report geometry from its current page only. Hidden pages cannot
contribute their large legacy size hints. The outer Settings scroll area owns
the available viewport, and no code may mirror a transient viewport or child
height into equal minimum/maximum heights on a shared stack.

Queued Settings routing and activation callbacks carry the MainWindow
navigation generation that created them. If another screen becomes current,
the callback expires without mutating the hidden Settings page. Existing tab
lifecycle hooks retain their refresh semantics; their queued work is simply
prevented from running against a screen that is no longer current.

The acceptance matrix includes first entry immediately after launch, deferred
Settings population, repeated family/task selection, Create/Replace assistant
entry, and resize at the 900x600 application minimum. The visible workflow and
its primary action must remain reachable, compact vertical scrolling is valid,
page-level horizontal scrolling and unintended top-level windows are not, and
settling the event queue must not change the selected page.
