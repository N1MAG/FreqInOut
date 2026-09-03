# Message Intelligence Projection Spec

## Intent

FIO must ingest message traffic from external tools and files into an FIO-owned
database projection before that traffic reaches Qt views. High-value
intelligence is part of the product, but it must be computed once, stored with
stable provenance, and reused through indexed queries. The UI must not parse
external databases, scan high-volume folders, or recompute message intelligence
while rendering a page.

This spec extends the source/projection/view rules in
`docs/internal/operational_view_framework_spec.md` and the protocol-neutral
source boundary in `docs/internal/protocol_neutral_comms_integration_spec.md`.

## Product Contract

- External tools remain sources of truth for their own records, files, and
  protocol-specific actions.
- FIO owns the normalized operational message projection, intelligence metadata,
  deletion audit, and cross-source dedupe state.
- Every source record must have a stable FIO `message_id` and at least one
  external reference.
- Every projection write must be idempotent by source identity and content
  fingerprint.
- Message views must query bounded indexed projections. Default views must favor
  recent, direct, actionable, and alerting traffic over historical bulk.
- Full migrated history remains available through FIO storage and explicit
  search/archive workflows, but it must not be eagerly materialized into live UI
  tables.
- Purge/delete must separate user intent from source-side execution: hide,
  archive, delete external source, delete all linked refs, or retain audit-only
  metadata.

## Source Identity

Each external tool instance is represented as a message source.

Required source fields:

- `source_id`: stable FIO key, opaque to UI.
- `source_family`: `js8`, `spotter`, `commstat`, `varac`, `flmsg`, `flamp`,
  `bbs`, `meshcore`, `meshtastic`, `reticulum`, `mqtt`, `aprs`, `local`,
  `import`.
- `source_label`: user-facing source/app/profile label.
- `radio_id` and `app_instance_id` when applicable.
- `endpoint_or_path`: external database, API endpoint, folder, file, or import
  reference.
- `capabilities_json`: receive/send/read/delete/archive/open/source-specific
  flags.
- `provenance_json`: RF-only, internet-assisted, mixed, imported, manual,
  store-and-forward, trusted/untrusted hints.
- `enabled`, `last_seen_utc`, `last_ingested_utc`.

## External Reference Keys

Each message may have multiple external references. A reference is unique by
`source_id`, `external_kind`, and `external_key`.

Recommended keys:

- JS8Call: inbox id when available; fallback `utc/from/to/text_hash`.
- FIOSpotter: spotter table id or report key.
- CommStat: artifact key.
- VarAC: GUID/message id plus mailbox/source context.
- FLMsg: normalized path plus content hash.
- FLAmp: Q ID, block id, transfer id, reconstructed file hash, and path.
- BBS: managed mailbox path plus content hash and archive/live context.
- Mesh/Reticulum/MQTT/APRS: protocol message id when available; fallback
  source timestamp plus content hash.

## Canonical Message Projection

`message_projection` is the hot table used by inbox, map context, attention
queue, compose handoff, purge review, and future operational intelligence.

Required fields:

- Identity: `message_id`, `canonical_key`, `content_hash`,
  `projection_version`.
- Source: `primary_source_id`, `source_family`, `source_label`, `radio_id`,
  `app_instance_id`.
- Actor/context: `from_call`, `to_call`, `group_name`, `scope`, `state_code`,
  `grid`, `lat`, `lon`.
- Time: `event_ts`, `received_ts`, `event_utc`, `received_utc`,
  `projected_utc`.
- Display: `message_type`, `display_type`, `status`, `severity`, `subject`,
  `summary`, `body_preview`.
- Intelligence: `topics_json`, `entities_json`, `actionable`,
  `operator_attention`, `confidence`, `recommended_action`,
  `intelligence_version`, `intelligence_utc`, `intelligence_json`.
- Lifecycle: `read_state`, `pinned`, `archived`, `deleted`, `deleted_utc`,
  `retention_class`.
- Search: `search_text`, indexed enough for bounded LIKE/FTS migration.

Large raw bodies, file payloads, and reconstructed artifacts belong in linked
tables, not the hot projection row.

## Artifact And FLAmp Contract

`message_artifact` stores file/form/attachment/transfer details.

FLAmp is a special artifact family:

- Preserve Q ID, block id, transfer id, sender, target, block count, missing
  blocks, reconstruction state, source path, reconstructed path, content hash,
  signature/hash verification, and VarAC retrieval linkage.
- A partial transfer may exist before a canonical human-readable message exists.
- VarAC retrieval logic may reference Q IDs and block state without reparsing
  FLAmp files.

## Delete And Purge Contract

Deletion is a queued operation with an audit trail.

Supported effects:

- `hide_fio`: remove from default FIO views, retain source refs.
- `archive_fio`: move local FIO-managed files to archive, retain projection.
- `delete_external`: delete the selected external row/file when supported.
- `delete_all_external_refs`: delete all linked source refs with capability
  checks.
- `audit_only`: purge raw body/artifacts while retaining minimum provenance,
  timestamps, source, and operator audit record.

Rules:

- The UI applies local tombstones immediately and queues external work.
- Source deletes are batched and capability-gated.
- Every external delete result is recorded with source id, external key, status,
  error text, and timestamp.
- Failed external deletes must not resurrect hidden FIO rows.
- Purge must not remove intelligence needed for operator history unless the user
  explicitly chooses audit-only minimization.

## Performance Rules

- Ingest and enrichment run off the UI thread.
- Projection writes are incremental and idempotent.
- Queries are bounded by time, limit, source, group, status, severity, and
  deletion/archive state.
- Default inbox queries must return in less than 250 ms on a typical production
  laptop with 100k projected messages.
- Detail views may lazy-load raw bodies/artifacts by `message_id`.
- Re-enrichment is triggered only when content hash, source metadata,
  intelligence version, or operator override changes.
- All projection APIs must be Qt-free and unit-tested with SQLite memory or temp
  databases.

## First Implementation Slice

- Add the FIO-owned message projection schema.
- Add idempotent source, message, external ref, artifact, and delete queue/audit
  helpers.
- Ensure schema during cold-start database initialization.
- Project existing unified message rows into the normalized FIO tables on a
  coalesced background worker.
- Preserve external references for JS8Call, FIOSpotter, CommStat, SitRep,
  VarAC, FLMsg, FLAmp, and BBS rows.
- Preserve file-backed artifact metadata, including FLAmp Q IDs and block ids.
- Mark projected rows deleted when existing source-specific delete actions
  succeed.
- Add contract tests for uniqueness, bounded queries, external refs, artifacts,
  and delete queue/audit behavior.
- Migrate the Messages tab to query this projection in a follow-up slice, then
  move source-specific ingestion into incremental checkpointed projection
  writers.
