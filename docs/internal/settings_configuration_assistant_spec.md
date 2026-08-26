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

### Operating Models

Problem: Operating Models are shared definitions, but their current placement
under Radios implies that they are radio-local.

Target behavior:

- Move Operating Model configuration/admin to `Settings -> Main`.
- Keep Operating Model Assignment under `Settings -> Radios`.
- Avoid modal-heavy editing in a later phase by converting model editing into a
  full-width editor surface.
- Explain that radios assign shared models.

### Schedule Assignment

Problem: endpoint details are overbaked in schedule assignment. By this point
the user should be choosing which plan a radio follows, not auditing control
ports.

Target behavior:

- Hide endpoint columns from the normal Schedule Assignment table.
- Keep RF Guard status visible.
- Move endpoint/path concerns to readiness, health, and configuration guidance.

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
- Add CommStat RF-only compose using the current CommStat/SuperSpotter format.
- Include CommStat brevity codes.
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

Target behavior:

- Make VarAC BBS configuration and status clear in Settings without mixing it
  with unrelated JS8/Spotter/CommStat concepts.
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
- Show a preview of the managed BBS structure before writing or publishing it so
  the operator can understand what callers will see.
- Preserve radio awareness where it matters, but avoid implying that shared BBS
  artifacts are duplicated per radio unless they truly are.
- Ensure Compose, Messages, Map, and health/status surfaces describe VarAC BBS
  traffic consistently.
- Add a configurable sweeper from VarAC BBS Inbox and FLMsg/FLAmp inputs into
  the Managed BBS. The matching model should support sender/from filters plus
  subject-contains filters and should allow one source rule to copy into
  multiple managed BBS locations.
- In Messages, preserve the existing `+BBS` action and add a clear way to remove
  FLMsg/FLAmp content from BBS sync without deleting the original message or
  source artifact. Initial implementation changes copied rows to a `-BBS`
  action that removes only the copied BBS artifact.
- Treat BBS repair and diagnostics as candidates for the future Tools UI.
