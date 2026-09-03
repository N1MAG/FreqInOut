# VarAC Managed BBS Database Manifest Spec

## Goal

FreqInOut multi-rig treats the Managed VarAC BBS Library as a normalized catalog of publishable artifacts and location memberships. A file can be available in multiple managed BBS locations without requiring duplicate library copies. The live VarAC BBS directory remains a materialized view that FIO publishes on demand for the active radio/session/location.

## Contract

- Applies to multi-rig code and databases only.
- Existing folder-based Managed BBS locations remain valid.
- The live VarAC BBS directory is disposable FIO output when Managed BBS is enabled.
- FIO must not require symlinks for correctness. Copy-on-demand is the cross-platform default.
- A DB-backed manifest must degrade to the existing folder scan path when the DB is unavailable or unseeded.
- Manifest generation must be read-optimized and avoid parsing or scanning large message folders during a visitor request.
- Purge policy and per-callsign visibility must be modelable without changing the live VarAC BBS folder contract.
- The BBS preview must show DB mapping intent when the catalog exists; raw folder contents are only a fallback preview.
- Publish and unpublish are database membership operations. FIO must not restore files to disk simply because an older live publish manifest once contained them.

## Data Model

### `bbs_artifacts`

One row per canonical publishable object.

- `artifact_id`: stable FIO identifier.
- `source_kind`: managed folder file, message artifact, FLAmp relay file, future source type.
- `source_id`: source context such as location id, message source id, or relay id.
- `source_path`: current local file path when the artifact is file-backed.
- `display_name`: operator-facing name.
- `size`, `mtime_ns`, `content_hash`: publish/change metadata.
- `q_id`, `block_id`, `metadata_json`: future message and FLAmp intelligence hooks.
- `deleted`: logical tombstone for purge workflows.

### `bbs_locations`

One row per managed BBS location.

- `location_id`, `name`, `source_dir`, `enabled`, `metadata_json`.

### `bbs_location_artifacts`

Many-to-many mapping from locations to artifacts.

- `location_id`, `artifact_id`.
- `live_name`: optional VarAC-safe name override.
- `sort_order`: listing order.
- `visibility_rule`, `retention_class`: future access and purge policy.
- `publish_enabled`: non-destructive membership toggle.

## Publish Flow

1. Background Managed BBS run syncs configured folder locations into the catalog.
2. When a visitor requests a location, FIO queries `bbs_location_artifacts` joined to `bbs_artifacts`.
3. FIO builds the live publish manifest from those rows plus virtual helper files.
4. FIO compares the generated manifest against the last live manifest.
5. FIO copies only missing or changed files into the live VarAC BBS directory.
6. FIO removes stale live entries owned by the previous manifest.

The database is the source of truth for membership. The live BBS folder is only what VarAC can list and serve at that moment.

## Preview And Control Contract

The Managed BBS preview shows the operator the catalog mapping for each location:

- Location identity, alias, access policy, retention policy, and source folder.
- Files currently published to that location according to `bbs_location_artifacts`.
- A clear empty state when a location has a catalog row but no published memberships.
- A fallback source-folder preview only when no DB catalog has been seeded yet.

Messages and Settings share the same membership model. Any file-backed message artifact that can be published to VarAC BBS opens the shared publish workflow:

- `Publish to BBS`: show all valid live and managed BBS targets with checkbox selection.
- Managed target selection must add or enable one `bbs_location_artifacts` row without copying into durable managed folders.
- Live target selection may still copy directly to the VarAC live BBS folder for compatibility.
- Already-published managed targets must be preselected from the database mapping.
- `Remove from BBS` must disable managed membership rows and delete only direct live copies created by FIO.
- `Purge`: apply the selected policy to mapping rows, FIO catalog rows, archives, and optional external source deletion.

The live VarAC BBS folder should be treated as generated output. It is reconciled from the DB manifest during active BBS publishing and should not be used as the user's durable file-management surface.

## Future Extensions

- FLAmp queue listings can be backed by indexed artifact rows instead of glob/stat scans.
- Per-callsign and per-group visibility can be applied at manifest query time.
- Purge can remove only membership, remove FIO catalog rows, archive files, or delete external source files according to policy.
- Optional link acceleration may be added later as `copy`, `hardlink`, `symlink`, or `auto`, with `copy` remaining the default.
