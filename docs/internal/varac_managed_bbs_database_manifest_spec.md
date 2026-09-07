# VarAC Managed BBS Database Manifest Spec

Status: Slice 2 implemented; automated exit gate passed 2026-09-06.

The production remediation and delivery gates remain governed by
`production_reliability_and_workflow_remediation_spec.md`.

## Goal

FreqInOut multi-rig owns one station Managed BBS catalog. A canonical artifact
can be published in multiple logical BBS locations without durable duplicate
library copies. Every enabled radio/VarAC instance projects that same catalog
into its own live BBS directory. Live directories and their per-instance
manifests are disposable generated output, not administration surfaces.

## Ownership And Compatibility Contract

- Catalog, logical locations, access policy, retention policy, and publication
  membership are station-owned.
- Radio profiles retain only the VarAC runtime path, live BBS directory,
  enablement/health, and a link to the station workspace.
- Existing radio-owned location lists are imported once as a union. Existing
  station rows win conflicts; the primary/active profile supplies legacy
  station defaults. Legacy profile fields remain intact for rollback.
- The schema and ownership import each require a verified backup before their
  first mutation and use independent idempotency markers.
- A runtime must carry its catalog database identity explicitly. A compatibility
  caller with no identity uses the folder-backed path and must never borrow a
  process-global database.
- Existing folder locations remain valid discovery sources. Folder discovery
  may add artifacts or update source state, but never re-enables an
  operator-disabled membership.
- Copy-on-demand remains the cross-platform live-publication mechanism; no
  symlink is required.

## Schema Version 2

### `bbs_artifacts`

One row per canonical file-backed object. In addition to identity, origin,
path, display name, size/mtime/hash, relay metadata, and the logical tombstone,
version 2 records:

- `source_state`: `present`, `missing`, or `deleted`;
- `missing_since_utc`;
- `last_reconciled_utc`.

Missing source state is independent from operator publication intent.

### `bbs_locations`

One row per station logical location:

- identity, display name, optional source folder, and enabled state;
- `parent_location_id` for the administration tree;
- `access_rule`;
- `retention_mode`: `manual`, `global_default`, or
  `expire_after_days`;
- `retention_days` and compatibility metadata.

### `bbs_location_artifacts`

Many-to-many location membership:

- optional live-name override, order, visibility, and compatibility retention
  class;
- `publish_enabled`, which records operator intent;
- `disabled_reason`: blank, `operator_disabled`, or `retention_expired`;
- calculated `expires_utc` and `last_reconciled_utc`.

Changing a location retention policy or a source modification time recalculates
existing mapping expiries. Expiration disables the mapping with
`retention_expired`; it does not delete the catalog row or source file.

## Reconciliation And Publication

1. Startup performs the backup-safe schema and ownership migration.
2. The background BBS job performs bounded, indexed source/retention
   reconciliation once for the station, then runs each radio projection.
3. Folder discovery upserts current files, marks absent files missing, and
   preserves disabled memberships.
4. Manifest queries exclude disabled locations, disabled/expired mappings,
   tombstones, and missing sources.
5. Each radio compares against a manifest keyed by its resolved live BBS path,
   copies only missing/changed output, and removes only stale output previously
   owned by that instance's manifest.
6. `current_publish_manifest.json` remains a diagnostics compatibility pointer
   to the most recent projection; runtime reconciliation uses the per-radio
   manifest.

If a missing source returns, its source state becomes present. A mapping the
operator unchecked stays disabled. A previously enabled mapping may resume;
retention is still enforced from the source modification time.

## Administration And Message Contract

Top-level `BBS` is the first-class station service. Its guided tabs are ordered
`Radio Service`, `Locations & Access`, `Publishing`, `Visitor Preview`, and
`Visitor Helpers`. It provides:

- a logical location tree with access, retention, and disabled state;
- progressive Add/Edit/Save/Disable location administration;
- callsign and access-code location rules with salted hashes rather than stored
  plaintext;
- a bounded newest-first artifact view with explicit Published, Expired, and
  Removed filters plus a visible publication checkbox;
- origin, source path, size, age/modified time, access, retention/expiry, and
  publication health details;
- compact reflow with opt-in details at 1000 pixels and below.

The `Radio Service` tab owns BBS-specific adapter controls for every configured
VarAC radio. VarAC Settings retains the native launcher, inbox, outbox, and
radio-specific inbound-safety configuration, plus a route to BBS. Existing
radio-profile fields remain valid adapter persistence and do not imply that BBS
administration belongs under the selected radio.

Visitor Preview reuses this surface in read-only mode. It filters the effective
location tree using public/hidden visibility, the entered caller callsign, and
station/location allowed-callsign policy; it then shows only effectively
published artifacts for the selected visible location.

Location Save is catalog-only: it never creates or scans a folder. Disable is
non-destructive. The selected location chip scopes the checkbox while the
bounded catalog-wide artifact list permits adding an existing artifact to a new
location. Membership edits remain in memory until `Apply Changes` commits them
atomically; `Revert` restores persisted state.

`Remove from BBS` disables every mapping for an artifact but retains its source
and catalog row. `Keep in BBS` sets the existing mapping `retention_class` to
`keep` and clears expiry. `Republish` restores the selected mapping with normal
retention and calculates a fresh expiry from action time. These actions require
no schema migration.

Messages `+BBS` opens the same logical location choices. Accepting the dialog
atomically replaces that artifact's station memberships; unchecking every
location removes publication everywhere while leaving the received source
file unchanged. No live directory is copied from or deleted on the UI thread.

Generated VarAC helper/navigation files are rendered only in `Visitor Helpers`.
They are excluded from the operator artifact publication table and have no
membership checkbox.

## Visitor Helpers

New visitor helper labels and physical filenames omit the confusing `.txt`
suffix. The first entry is `00 HOW TO USE - Type command then refresh BBS`.
Helpers no longer promise a fixed ten-second delay. Asynchronous actions say:
`Request sent—refresh when the updated listing is ready`. Historical `.txt`
helper filenames remain recognized so old generated files can be filtered and
reconciled when an extensionless projection supersedes them.

The legacy `Reset To Default` operation resets only transient live visitor
navigation. If exposed, it is named `Return Live BBS Home` and explicitly states
that configuration, membership, and source files are unchanged.

## Automated Acceptance Record

- Version 1 to 2 migration: verified backup, rollback on backup failure,
  preserved rows, and idempotent marker.
- Legacy radio union: verified backup, station-row precedence, retained legacy
  data, and idempotent import marker.
- One and two-radio projections: identical station content with distinct live
  directories and manifests.
- Retention: expiry disables publication without deleting source; policy edits
  recalculate existing mapping expiry.
- Missing source: publication stops, reconciliation is bounded, and folder
  discovery does not restore operator-disabled membership.
- UI/message membership: checkbox add/remove and uncheck-all are atomic and
  source-preserving.
- Performance sample: 10,000 mappings, bounded 200-row administration query;
  p50 2.59 ms, p95 2.77 ms, max 2.82 ms on the development Mac.
- Visual review: Light/Normal at 1200x800 and Dark/Large Text at 900x560;
  compact details remain reachable without consuming the default artifact
  workspace.
- Refinement contract: Keep survives recalculation/reconciliation, Republish
  uses an action-time retention window, Remove preserves source/catalog, staged
  publication edits require Apply, and extensionless helpers retain historical
  cleanup compatibility.
- Refinement gate: 144 focused BBS tests pass with one environment skip; all
  172 repository test files pass in isolated processes with two skip-only files.
