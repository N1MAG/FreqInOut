# Local Nets And Resources LN-0 Architecture Decisions

Status: accepted contract for LN-1 implementation

Date: 2026-09-09

Governing documents:

- `local_nets_tools_resources_spec.md`
- `local_nets_tools_resources_implementation_plan.md`
- `ln0_ui_geometry_contract.md`
- `multirig_product_ui_contract.md`

## Purpose

This record closes the architecture decisions required by LN-0. It is based on
the current call-site, schema, UI, and test audit. It changes no runtime schema,
configuration, or production data.

## Current Ownership Findings

- `db_initializer.py` is the startup schema owner, but `net_schedule_tab.py`
  currently duplicates schema assurance, repair, bundled synchronization,
  migration, deduplication, and deletion from the UI layer.
- HF Nets is the primary `net_resources` writer. FreqPlanner is a second direct
  writer and its linked plan/resource update is not one transaction.
- `daily_schedule_tab.py` owns a separate `hf_schedule_resources` library whose
  integer IDs overlap the numeric namespace used by `net_resources`. A bare
  `resource_id` is therefore not a globally stable resource identity.
- `known_operating_groups.py`, HF projections, FreqPlanner, HF Nets, Settings,
  and Ops Center all consume some portion of the legacy resource shape.
- SchedulerEngine reads the commandable schedule tables, not `net_resources`.
  This allowlisted schedule load is the Local Nets isolation boundary.
- Settings `local_net_profiles` rows provide group/resource/mode/target metadata
  to current SOP workflows. They contain no recurrence, timezone, occurrence,
  stable identity, or accepted snapshot and are not Local Net schedules.
- Configured Operating Groups are Settings rows grouped by mutable normalized
  name. Multiple frequency/mode rows may represent one logical group, and no
  stable group identity currently exists.

The detailed UI evidence and responsive wireframes are in
`ln0_ui_geometry_contract.md`. The exact source call-site inventory is retained
in the LN-0 work-log entry and implementation review evidence.

## Locked Identity Decisions

All canonical cross-screen identities are nonempty immutable `TEXT` keys. Local
integer primary keys may be used as internal query accelerators, but they never
cross a UI/service boundary without their namespace and are not persisted as the
canonical relationship.

Canonical key fields are:

- `source_key`;
- `frequency_resource_key`;
- `net_entry_key`;
- `net_session_key`;
- `operating_group_key`;
- `local_net_schedule_key`; and
- `sop_profile_id` for the existing SOP profile identity.

New station-owned objects use a UUID-based key with a type prefix. Bundled and
imported objects use a stable source-owned external key when one exists, or a
UUIDv5 derived from source key plus normalized source identity. A content hash
is a version, not an object identity. Renaming an object therefore does not
change its key.

Legacy imports use explicit deterministic keys and a mapping table. They must
not infer that `net_resources.id = 7` and `hf_schedule_resources.id = 7` are the
same object.

## Operating Group Identity

LN-1 adds `operating_group_key` to every configured Operating Group dictionary.
Every row sharing the same normalized logical group name shares one key. Existing
valid keys are preserved. A legacy name without a key receives a deterministic
UUIDv5 key during the reviewed additive migration; subsequent Settings edits
must preserve that key, including a rename.

A Qt-free Operating Group identity adapter owns normalization, key assignment,
lookup by key/name, alias preservation, and immutable option models. UI code may
display a current name, but new resource and schedule relations persist:

- `operating_group_key`; and
- `group_name_snapshot`.

The Settings list remains the configuration owner in this feature. The resource
catalog must not create a second group editor or infer configured membership
from imported resource rows.

## Canonical Store Boundaries

The initial canonical tables live in `freqinout_nets.db`, beside the schedule and
legacy resource data they reference:

- `resource_catalog_sources`;
- `frequency_resources`;
- `frequency_resource_group_links`;
- `net_directory_entries`;
- `net_directory_entry_group_links`;
- `net_directory_sessions`;
- `legacy_net_resource_map`; and
- `resource_catalog_migration_state`.

LN-4 adds:

- `local_net_schedules`; and
- `local_net_occurrence_state`.

LN-5 adds normalized `sop_action_context_refs` only if action-level context is
required. The minimal schedule-to-SOP link is
`local_net_schedules.sop_profile_id`. Runtime occurrence context is carried in a
typed navigation intent and is not written as a new SOP profile or action.

`db_initializer.py` is the only startup schema-assurance caller. Qt-free stores
and migrators own transactions and data changes. No QWidget or tab may create,
alter, drop, deduplicate, import, or repair a table.

## Legacy Resource Classification

Each `net_resources` row always remains represented in the audit mapping:

- a usable frequency produces a Frequency Resource;
- a credible named net plus usable session fields may also produce a Net
  Directory Entry and Published Session;
- a general calling-frequency/digital standard produces no false net identity;
- an ambiguous or malformed row is marked `review_required` with field-level
  diagnostics rather than discarded.

`legacy_net_resource_map` contains the legacy table/ID namespace, canonical
keys, classification, source-row hash, diagnostic state, and migration time.
The existing SitRep seasonal JSON is a legacy compatibility/import source, not
the new national regulatory reference package.

## `local_net_profiles` Compatibility Decision

Legacy `local_net_profiles` remains readable until LN-5 qualification because
existing SOP editors and message-actionability logic consume it. It is never
converted directly into a Local Net schedule.

During Resources migration:

- a profile with a safely parseable operational target may seed a station
  Frequency Resource and group association after dry-run review;
- an unparseable target remains unchanged in Settings and is reported for
  operator review;
- no recurrence or occurrence is invented; and
- no SOP is activated or scheduled from the profile.

After canonical UI parity, a compatibility adapter projects canonical local
resource choices to existing SOP consumers. Removal of `local_net_profiles` is
out of scope until all readers are proven migrated by a later removal gate.

## HF Subscription Compatibility

`net_schedule_tab.resource_id` remains unchanged for rollback. LN-1 adds nullable
canonical fields:

- `net_session_key`;
- `accepted_session_version_hash`;
- `accepted_resource_version_hash`; and
- `accepted_snapshot_json`.

Schedule and operational projections carry the canonical keys alongside the
legacy identifier. Applying a directory update always enters the existing HF
save, plan reprojection, RF Guard, SOP conflict, and scheduler refresh path.

## Migration And Cutover State Machine

The catalog uses these authority states:

1. `legacy` — current tables and UI are authoritative.
2. `shadow_ready` — LN-1 schema and reviewed import exist, but legacy writers
   remain authoritative and Resources navigation is absent.
3. `canonical` — LN-2 has performed a final delta import and routed every known
   writer through the canonical repository in one cutover.

LN-1 does not claim canonical ownership while HF Nets or FreqPlanner can still
write legacy state. Before LN-2 switches authority it must:

1. make a fresh backup;
2. re-run the idempotent dry-run and import any legacy delta;
3. prove reader parity;
4. install the canonical writer/projection adapters;
5. set `canonical` in the same transaction; and
6. expose Resources only after the transaction succeeds.

Backup or cutover failure leaves authority `legacy`/`shadow_ready`, leaves the
old UI usable, and exposes no partially writable Resources screen. Read-only
catalog queries never initiate migration.

## Bundled Reference Decision

The new bundled reference manifest initially contains versioned, cited national
US Amateur and GMRS reference data only. It contains no local repeater, local
net, or fictional example records. Each source records provenance URL, rule
citation, verification date, effective/version date, and content hash. Updating
the package creates reviewable resource versions and never silently changes a
station schedule.

The reference validator provides advisory results only and does not determine
an operator's legal authorization.

## Navigation And Responsive Decisions

- The full master label is `Plans`, not `Plan Builder`.
- Compact Plans opens the Plans flyout rather than routing directly to Plan
  Builder.
- Resources is a separate master with an owned catalog/library icon and opens a
  flyout; it does not route directly to one child.
- New destinations remain lazy and are not startup-prewarmed.
- The 900x560 requirement is an outer-window target. The current 900x600 minimum
  is reduced during LN-2, the first shell-touching package; LN-0 wireframes are
  accepted against the target rather than treating the current minimum as a
  product exception.
- Cross-screen operations use a typed, Qt-free navigation intent carrying stable
  identity, draft, return route, query/filter, selection, and scroll context.

## Local Nets And SOP Command Boundary

Local Nets use a dedicated projection with `source_type=LOCAL_NET` and
`commandable=false`. SchedulerEngine's schedule-table allowlist is protected by
an executable characterization test. Local occurrences never enter generic QSY
metadata or HF/SOP priority arbitration.

Opening a linked SOP uses `local_net_schedule_key`, optional `net_session_key`,
and the runtime occurrence key in the navigation intent. It opens contextual SOP
guidance but does not activate the SOP. Any future automatic activation requires
a separate approved policy and specification.

## Gate Resolution

LN-0 is eligible to pass when the audit/geometry records and characterization
tests demonstrate:

- every known legacy reader/writer and duplicate schema owner is listed;
- all open identity, compatibility, navigation, viewport, bundle, and SOP
  payload decisions above are locked;
- fixtures cover empty, bundled, custom, duplicate, malformed, linked,
  unlinked, and named-source cases;
- SchedulerEngine isolation is executable; and
- baseline focused tests and timings are recorded without touching production
  configuration.

Passing LN-0 authorizes LN-1 only. It does not authorize navigation exposure,
canonical cutover, legacy deletion, or production-data testing.
