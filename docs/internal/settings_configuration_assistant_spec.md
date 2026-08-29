# Settings Configuration Assistant Spec

Status: planned, with first UI cleanup slice in progress for multi-rig 2.0.0
private testing.

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

## Phase 4: Guided FreqPlanner

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

VarAC BBS is a P2 workstream. The existing managed BBS vault, access-code,
cluster, and location handling are useful groundwork, but the operator workflow
needs to read as a complete message lifecycle instead of a collection of expert
settings.

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
- Make scope obvious on every VarAC page. `Radio Paths` and `Radio Live BBS`
  are radio-specific. `Shared Library`, `Visitor Preview`, `Shared Sweeper`,
  and `Access Guard` describe shared BBS management unless a control explicitly
  says it is publishing to the selected radio.
- Rename `VGuard` in operator-facing UI to `BBS Access Guard`. The function is
  inbound file protection based on sender trust; it is separate from the Managed
  BBS Library and from message-signature/hash verification.
- The configured-radio selector above Settings should stay compact enough that
  dense configuration pages remain usable. It should identify the selected radio
  and expose activation/default/app-edit actions without consuming the page.

Target behavior:

- Make VarAC BBS configuration and status clear in Settings without mixing it
  with unrelated JS8/Spotter/CommStat concepts. Initial implementation splits
  VarAC administration into persistent `Radio Paths`, `Radio Live BBS`, `Shared
  Library`, `Visitor Preview`, `Shared Sweeper`, and `Access Guard` tabs inside
  the VarAC settings area.
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
  VarAC BBS, FLMsg, and FLAmp sources plus a radio-scoped Settings review surface;
  background copy application remains a future slice.
- In Messages, preserve the existing `+BBS` action and add a clear way to remove
  FLMsg/FLAmp content from BBS sync without deleting the original message or
  source artifact. Initial implementation changes copied rows to a `-BBS`
  action that removes only the copied BBS artifact.
- Future Settings/Messages work should add age-based archive sweepers for
  original FLMsg and FLAmp receive folders. This is separate from BBS copy
  removal: the user should be able to keep radio-message archives clean without
  confusing that with deleting or unpublishing BBS copies.
- Treat BBS repair and diagnostics as candidates for the future Tools UI.
