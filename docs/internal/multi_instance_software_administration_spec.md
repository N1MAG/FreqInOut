# Multi-Instance Software Administration Specification

Status: MIS-0 through MIS-5 automated gate passed. The 2026-09-17 Add Radio
operator run reopened distinct-instance integration qualification: the shared
pure model was not enforced by every production Add Radio path. GRS-6.1 now
enforces shared inventory, stable draft identity, source-locked import, and
explicit clone semantics on both production routes. GRS-6.2 adds one resolved
JS8/Fast Light recipe contract shared by Add Radio, Software Administration,
native planning, launch review, and persistence. GRS-6.3 through GRS-6.5 now
pass their automated gates, including final-save inventory revalidation and
atomic cross-family persistence. Release remains blocked on the repeated
operator-assisted live route in `guided_radio_software_configuration_spec.md`.

Governing delivery contract: `project_delivery_rules.md`

Governing product/UI contract: `multirig_product_ui_contract.md`

Related specifications:

- `settings_configuration_assistant_spec.md`
- `guided_radio_software_configuration_spec.md`
- `js8call_modern_variant_compatibility_spec.md`
- `multi_endpoint_scheduler_concurrency_spec.md`
- `production_reliability_and_workflow_remediation_spec.md`

## Product Outcome

Software Administration must let an operator start with the software they want
to configure, see every radio that uses it, and safely add, import, review,
launch, and persist a distinct instance without reconstructing application
internals by hand.

The normal workflow is:

`Software -> one application family -> one radio -> create/use/replace one instance -> review -> save -> verify`

The operator must be able to understand, before applying anything:

- which radio owns the instance;
- whether FIO discovered, imported, or manages its launch recipe;
- which executable, profile/configuration, endpoint, data paths, and launch
  identity distinguish it;
- whether ports and storage are unique or intentionally shared;
- what FIO will write and what remains operator-owned;
- whether the saved instance has been merely configured, detected, reached, or
  semantically verified.

This work is additive and non-destructive. Discovery never writes. Importing an
existing instance never rewrites its external configuration. `FIO-managed`
means FIO owns the durable instance identity and launch recipe; it does not mean
FIO owns a third-party application's native settings. This assistant invokes no
external configuration writer. Any future writer requires an explicit review,
backup, apply, and readback-verification contract.

## Supported Instance Families

### JS8Call

FIO supports stock JS8Call 2.2.0, JS8Call Improved 3.0.3, and Subspace as
rig-scoped multi-instance applications. Each local instance requires:

- a stable FIO system key and operator-facing instance name;
- a stable, unique `--rig-name` launch identity;
- a unique local TCP API endpoint and UDP port where UDP is enabled;
- a reviewed settings/profile source;
- a rig-scoped application-data root and attributable message files;
- a distinct launch-bundle identity and radio assignment.

Per maintainer direction, Subspace is assumed to instantiate and isolate second
instances in the same fashion as JS8Call 2.2.0 and Improved 3.0.3. The former
single-local-instance/shared-store restriction is removed. Runtime evidence that
contradicts the assumed rig-scoped root must produce `Needs attention`; FIO must
not silently attribute one shared file source to multiple radios.

### Fast Light

A Fast Light instance is a radio-scoped workflow containing FLRig and FLDigi,
with FLMsg and FLAmp shared by default unless the operator chooses advanced
radio-specific paths. A distinct instance requires:

- unique FLRig and FLDigi control endpoints;
- distinct native profile/config roots where supported;
- separate FLDigi log/check-in paths when attribution matters;
- an explicit FLDigi-to-FLRig relationship;
- persisted executable paths and effective launch commands;
- application/version-qualified launch arguments and readiness checks.

FIO must not claim to have created an application-native managed profile when a
supported writer or installed-version capability has not been verified. The
current assistant may manage the FIO launch recipe while the native profile
remains operator-configured and explicitly reviewed.

### Cluster VarAC

Each VarAC radio uses a distinct node record with its own installation/launcher,
INI, database/runtime paths, inbox/outbox paths, and launch identity. Cluster
membership is a separate, persisted relationship that supplies cluster ID,
instance number, shared database where applicable, counter refresh, gateway
handler, and PTT-lock policy.

FIO guides the operator through node creation first and cluster membership
second. It does not imply that ordinary single-instance VarAC requires cluster
mode. It must detect duplicate node paths, launch identities, and cluster
instance numbers before saving.

An additional VarAC node defaults to standalone even when another node or
cluster exists. Cluster create/join is an explicit operator branch; discovery
of existing VarAC or cluster data does not select it.

### Station-Shared Supporting Services

FIO Spotter is built into FIO. Its station MCF catalog is resolved by FIO and is
not an external software instance, per-radio launch item, or normal-flow folder
choice.

CommStat is a station-shared service by default. One durable CommStat process
identity may have multiple bindings, each naming an exact radio-owned JS8
instance/endpoint and capability scope. Adding a radio creates a binding rather
than another CommStat process. The launch planner starts the shared identity
once and never deduplicates distinct radio bindings by process name alone.

## Durable Instance Manifest

Existing application-specific tables remain authoritative for operational
settings and radio links. An additive software-instance manifest records the
cross-application lifecycle evidence that those tables do not share:

- `instance_key`: stable FIO identity, never derived from row position;
- `family_key` and linked application `system_key`;
- ownership: `operator`, `fio_managed`, or `remote`;
- provenance: manual, detected file/profile, running endpoint, clone, or managed;
- executable, configuration path/root, data root, effective launch command;
- host plus named TCP/UDP endpoint claims;
- serial, rig-control, PTT, log, database, and storage resource claims;
- desired and last-observed configuration fingerprints;
- verification state, summary, evidence, and timestamps;
- bounded family-specific metadata.

The manifest does not duplicate the radio assignment. The radio profile's
existing application link remains the source of truth. New runtime instances
cannot be created without an owning radio. Historical, imported, replaced, or
disassociated records may remain unassigned for recovery, but are inactive and
are not part of the normal creation path.

All manifest JSON is bounded, versioned, normalized, and free of credentials.
Additive schema assurance creates missing columns/tables without transforming or
deleting existing production values.

## Discovery And Adoption

`Find existing configurations` is explicit and asynchronous. It scans only known,
bounded application locations and already-loaded FIO configuration. It must not
walk an unbounded home directory, contact radios, or block the UI thread.

Each candidate shows:

- application/variant and instance/profile name;
- executable and configuration source;
- host and named ports;
- data/message/log paths;
- matching FIO record or radio, if any;
- confidence and evidence;
- conflicts and the proposed operator action.

The source choices are `Set up a new local instance (FIO-guided)`, `Find or
import an existing installation`, and `Connect to a manual or remote instance`.
A discovery result must be selected explicitly before it can be imported.
Ambiguous candidates are review-only and are never assigned automatically.

Import captures current configuration and a fingerprint. It does not take
ownership or modify the source. A subsequent difference is drift, not permission
to overwrite.

## Managed Creation And Port Policy

FIO proposes ports from family-specific ranges after checking:

1. persisted application records;
2. saved launch bundles and manifests;
3. all ports proposed in the current transaction;
4. every retained unsaved draft in the guided session;
5. family-internal overlap, such as FLRig and FLDigi claiming one TCP endpoint.

Add Radio and Software Administration consume the same immutable inventory
snapshot and proposal generation. A distinct proposal may reuse an installed
binary but not an existing native profile, data/message root, endpoint,
manifest, or launch identity. An imported candidate is source-locked; changing
an identity field requires `Clone as distinct`.

Live reachability is checked explicitly from Health after save. It is not
performed while navigating the assistant and is not treated as proof that the
responding process is the expected service. This avoids blocking the UI and a
time-of-check/time-of-use claim that a currently free port has been reserved.

Remote endpoints are checked for persisted collisions but are not treated as
locally reservable. A listening port proves reachability only; semantic
verification must still identify the expected service.

FIO never silently renumbers an imported instance. A conflicting imported port
is shown with `Use another port`, `Link to existing owner`, or `Keep external and
repair manually` choices.

Port checks include JS8Call TCP/UDP, FLRig XML-RPC, FLDigi XML-RPC, and any
configured rigctld or application-specific endpoint. Serial devices, PTT groups,
message roots, log roots, VarAC INI/database paths, and cluster instance numbers
are first-class resource claims even though they are not TCP ports.

## Save And Apply Contract

Before Save, the review page lists every durable value and external action.

Validation occurs before mutation. FIO then saves the application record,
manifest, radio link, optional Cluster VarAC membership, and launch-bundle
identity as one atomic database operation. A failure rolls the complete change
back. The current assistant does not modify external files. Any future supported
external writer must use this staged plan:

1. validate and preview;
2. back up the explicit target;
3. apply through an application-specific writer;
4. verify readback;
5. commit FIO ownership/evidence;
6. retain a clear recovery path if launch verification later fails.

Unsupported external writers never receive a generic best-effort rewrite.

### Assignment cardinality and replacement

The durable invariant is one-to-one within a software family:

- one radio has at most one JS8Call instance, one Fast Light instance, and one
  VarAC instance;
- one runtime instance is assigned to at most one radio; and
- a radio may use one instance from each different family because FIO's normal
  TriMode workflow can legitimately configure JS8Call, Fast Light, and VarAC for
  the same physical radio.

Every assignment path uses the same store service. Creating a new orphan runtime
instance is blocked. If no radio exists, the primary action is `Create a radio
first`. Existing unassigned records remain available through a bounded recovery
picker and cannot launch, ingest, or participate in a VarAC cluster while
unassigned.

An occupied family slot is never silently overwritten and the operator is not
required to disassociate it first. `Replace instance` shows the current and
proposed identities before entry and again at Review. The store receives the
expected current instance ID, validates the complete proposed state, then swaps
the radio link, launch identity, and optional cluster membership in one
transaction. A stale selection, collision, or write failure rolls back the whole
operation and leaves the current assignment operational. The replaced record is
retained disabled as a previous configuration for recovery; external files are
not changed.

`Disassociate` is an explicit Advanced action. It clears only the selected
family's radio link/use flags, FIO launch items, and applicable VarAC membership;
it disables and retains the application record and manifest. Deleting the
external application, profiles, logs, messages, database, inbox, or outbox is a
separate operation and is never implied by disassociation.

## Launch And Verification

Startup and manual launch use the same persisted station launch planner. The
effective launch preview shows radio, instance, executable, arguments, endpoint,
config/profile, storage/data root, ownership, dependencies, and conflicts.

Endpoint-scoped applications are not deduplicated merely because their process
names match. Shared tools are deduplicated only when their persisted instance
identity is intentionally shared.

Readiness states have precise meanings:

- `Configured`: durable settings exist.
- `Detected`: external evidence exists but is not yet adopted.
- `Reachable`: something answered at the endpoint.
- `Verified`: the expected service/instance and resources match.
- `Managed by FIO`: FIO owns supported generated configuration.
- `Operator managed`: FIO observes but does not rewrite it.
- `Needs attention`: collision, drift, ambiguous storage, or identity mismatch.

A slow or failed instance remains isolated in its endpoint worker/readiness lane
and cannot delay other radios or receiver endpoints.

## Software Administration UX

The software-family workspace retains the existing three-level chip model:

1. software family;
2. radio context;
3. configuration task.

Each level is a true exclusive, non-empty selection group. Exactly one family is
active; selecting or re-selecting a chip cannot leave multiple highlighted
choices or visually clear the current choice. This is navigation state, not a
claim that a radio can use only one family.

The primary action is contextual. An available radio shows `Create or use
instance`; an occupied family slot shows `Review current` and `Replace
instance`; no configured radio shows `Create a radio first`. `Assign existing`
opens a family-filtered picker containing only compatible unassigned records.
The assistant is a responsive in-workspace step surface rather than a dense
all-fields dialog.

Steps are `Purpose`, `Find or create`, `Identity`, `Connections`, `Files`,
`Launch`, and `Review`. Completed steps use concise text/icon state. At compact
sizes, only the active step expands; the review remains vertically scrollable
without page-level horizontal scrolling.

Normal fields use operator names. Internal IDs, source keys, hashes, and raw JSON
remain in Advanced details. No placeholder or example contains a real or
plausible callsign.

The normal instance detail surface shows assignment, management ownership,
endpoint, configuration, data/storage, launch policy, and last verification.
Advanced reveals fingerprints and bounded evidence.

## Slices And Exit Gates

### MIS-0 — Specification And Existing-Work Checkpoint

- Commit the completed Software Administration workspace as a recovery point.
- Establish this lifecycle, persistence, safety, and UX contract.
- Record the Subspace rig-scoped product assumption.

Exit gate: checkpoint commit exists; specification is internally consistent and
requires no destructive migration.

### MIS-1 — Core Model And Additive Persistence

- Add manifest dataclasses, normalization, bounded JSON, and collision rules.
- Add additive manifest persistence and round-trip tests.
- Project manifests into application/radio records without changing legacy
  compatibility behavior.

Exit gate: fresh and upgraded database tests pass; invalid family/ownership,
duplicate identity, malformed JSON, and unsafe collision cases fail clearly;
existing records are unchanged.

### MIS-2 — Discovery, Adoption, And Port Planning

- Produce bounded candidates for JS8Call and configured FIO Fast Light/VarAC
  instances.
- Add stable import/adoption and managed proposal services.
- Check configured endpoint/resource collisions and require explicit Health
  verification for live reachability.
- Apply the Subspace rig-scoped launch/storage contract.

Exit gate: discovery performs no writes; ambiguous results are not auto-linked;
second-instance proposals are deterministic; collision and Subspace tests pass.

### MIS-3 — Guided Software UX And Persistence Integration

- Add the in-workspace assistant and instance detail review.
- Support JS8Call, Fast Light, and VarAC node/cluster paths.
- Save through explicit Settings-host persistence and refresh cached snapshots.
- Keep scans/checks off the UI thread and protect against stale completion.

Exit gate: all families render at 1920x1080, 1000x700, and 900x560 in Normal and
Large Text/light and dark themes; cancel makes no changes; saved values survive
restart; focus/navigation remain responsive.

### MIS-4 — Launch Integration And Final Qualification

- Persist/preview distinct launch identities and readiness evidence.
- Verify endpoint-scoped multi-launch behavior and drift reporting.
- Update operator help, related specs, and work log.

Exit gate: automated focused and adjacent Settings/launch/database tests pass;
`py_compile` and `git diff --check` pass. Linux operator qualification remains a
separate external gate for two simultaneous instances of each installed family,
including real radio/PTT resource behavior.

### MIS-5 — Radio-First Ownership And Atomic Replacement

- Make family, radio, and task chips true exclusive non-empty selectors.
- Remove `Not assigned yet` from normal creation and route an empty station to
  `Create a radio first`.
- Show `Available` or `Assigned: <instance>` before the operator enters instance
  details.
- Provide explicit new, assign-existing, and replace paths with current/proposed
  review.
- Centralize one-radio/one-instance-per-family enforcement for every write path.
- Add rollback-safe replacement and non-destructive disassociation, including
  launch-item and VarAC-membership cleanup.
- Keep legacy unassigned/shared records visible as `Needs attention`; never
  transform or delete them automatically.

Exit gate: exactly one chip at each active selection level after arbitrary and
repeat click sequences; a new instance cannot be saved without a radio; the same
runtime instance cannot be assigned to two radios; failed/stale replacement
leaves the prior application, radio link, launch recipe, and cluster membership
unchanged; successful replacement leaves no obsolete FIO-managed startup item
or active cluster membership; disassociation changes no external files; responsive light/dark,
Normal/Large Text checks pass at 1920x1080, 1000x700, and 900x560; focused and
adjacent Settings/store/launch tests, `py_compile`, HTML parsing, and
`git diff --check` pass.

Corrective lifecycle gate: once the embedded assistant is opened from either
`Create or use instance` or `Replace instance`, host snapshot and editor
refreshes must preserve it as the visible `QStackedWidget` page without changing
its draft or passive status. It is never promoted to a dialog or other top-level
window. Cancel restores the refreshed All-family summary or exact radio task;
successful completion closes only after persistence reports success.

Systemic geometry and deferred-navigation gate: Settings, the Software
Administration editor host, and the instance assistant use a current-page-only
stack sizing contract. The outer Settings scroll area owns the viewport; no
refresh or resize path may hard-pin a shared page/stack minimum and maximum to a
transient height. At compact sizes, vertical scrolling is permitted and the
active editor's primary action must remain reachable without horizontal page
scrolling. Deferred Settings routing and activation work must be discarded when
its navigation generation is no longer current. Exercise this gate from a real
MainWindow first-load sequence at the 900x600 application minimum as well as the
standalone Settings layout matrix.

## Model Assignment

- High-reasoning primary: architecture, additive schema, migration safety,
  concurrency/lifecycle integration, delegated-diff review, final tests/gate.
- `gpt-5.6-terra` high: bounded core discovery, launch validation, and focused
  tests.
- `gpt-5.6-luna` high: bounded guided UI/responsive implementation and focused
  UI tests.
- A cost-appropriate Terra/Luna package performs independent focused test and
  regression review after integration.
