# Message Projection Implementation Plan

## Goal

Move Messages, Map, attention, compose handoff, and purge workflows onto an
FIO-owned normalized message projection so intelligence stays rich without
making the UI perform ingestion or enrichment work.

## Phase 1: Projection Spine

Status: implemented.

- Add `message_sources`, `message_projection`, `message_external_refs`,
  `message_artifacts`, `message_delete_queue`, `message_delete_audit`, and
  `message_projection_checkpoint`.
- Add Qt-free upsert/query/delete queue helpers.
- Ensure schema during cold-start database initialization.
- Add tests for idempotent upsert, bounded query, external references, FLAmp Q
  IDs, delete tombstones, and DB initialization.

Acceptance:

- Projection APIs are Qt-free and unit-testable.
- Source refs are unique by `source_id`, `external_kind`, and `external_key`.
- Default list queries are bounded and exclude deleted/archived rows unless
  requested.
- FLAmp artifacts preserve Q ID, block id, transfer id, and transfer state.
- Delete requests tombstone the FIO projection immediately and retain audit.

## Phase 2: Incremental Projectors

Status: checkpointed row-backed all-source projector implemented; source-native
incremental checkpoints remain.

Populate the projection spine from the existing unified message row builder
without changing the UI read path yet. This gives every currently rendered
message a durable FIO `message_id`, source record, external reference, and
message intelligence row while keeping external sources authoritative.

Implemented:

- All current Messages row families project into `message_projection`.
- JS8Call, FIOSpotter, CommStat, SitRep, VarAC, FLMsg, FLAmp, and BBS rows get
  stable external reference keys.
- FLMsg/FLAmp/BBS file rows get linked artifact rows; FLAmp captures Q ID,
  block id, transfer id, transfer state, path, mtime, size, and content hash.
- Projection writes run on a coalesced background Qt worker after row build.
- Projection batches store a durable `message_projection_checkpoint` fingerprint
  and skip unchanged refreshes unless the operator forces refresh.
- Existing source-side delete success marks projected rows deleted and queues an
  auditable projection delete entry.
- Perf telemetry separates `messages.project_rows` from row build and rendering.

Remaining source-native projectors:

- JS8Call projector: inbox rows and raw directed traffic.
- FIOSpotter projector: decoded forms, report keys, Expect/auth metadata.
- CommStat projector: artifact keys, status, scope, reach, source refs.
- SitRep projector: fused event rows and report keys.
- VarAC projector: messages, VMail, BBS links, callsign stats references.
- FLMsg/FLAmp projector: files, forms, FLAmp Q IDs, block/transfer state.
- BBS projector: live/archive mailbox entries with source-side capabilities.

Acceptance:

- Projected row writes are idempotent by external ref and content hash.
- Existing source tables/files remain authoritative and are not deleted.
- Delete success hides projection rows immediately and retains audit.
- Source-native checkpoints avoid full rescans when no source changed.
- Enrichment jobs rerun only when content hash or intelligence version changes.

## Phase 3: Message UI Read Path

Status: projection-first Messages read path implemented; rich source-native
detail remains a background refresh/fallback behavior.

Move the Messages tab from source-list row building to projection queries.

Implemented:

- Query `message_projection` for the default inbox.
- Convert projected rows into the existing `UnifiedMessage` table model without
  reparsing external message stores.
- Render normalized projected detail text from hot projection fields.
- Lazy-load linked source refs and artifacts by `message_id` for projected detail
  panes.
- Bulk-load projected external refs with the inbox query so file-backed FLMsg,
  FLAmp, VarAC/BBS, and related rows can open through the native file renderer
  without per-row database calls.
- Mark projected messages read directly in the hot projection table.
- Hide/delete projected rows through the projection delete queue and audit path.
- Use loaded file refs for projection-only file open, live BBS archive, and file
  delete actions while keeping table paint/click handling in-memory.
- Keep source row building as a background projection refresh feeder.

Remaining:

- Use non-file source refs for JS8, Spotter, CommStat, SitRep, and VarAC
  source-native open, source-native mark-read/delete, and compose handoff when
  the row is loaded only from projection.
- Keep bounded live views, with explicit history/search controls for older
  traffic.

Acceptance:

- Opening Messages does not parse external records.
- Default Messages query returns quickly with 100k projected rows.
- Filtering by source, group, status, severity, search, and focus uses indexed
  projection fields.
- CommStat/SitRep intelligence remains visible without per-row recompute.

## Phase 4: Intelligence And Purge Workers

Status: core queue processor and Messages-tab bounded queue scheduling
implemented for FIO hide/source tombstones, audit-only minimization, and
file-delete capable external refs; every source-native external delete handler
remains.

Move rich intelligence and destructive source actions into background services.

- Materialize topics, entities, geography, severity, actionable state, and
  recommended actions.
- Preserve operator overrides and re-enrichment versioning. Remaining.
- Process delete queue in source-capability-gated batches. Implemented for
  scheduled `hide_fio`, `source_delete`, `audit_only`, and file-backed
  `delete_external`.
- Record external delete/archive results in audit rows. Implemented for
  supported effects.

Acceptance:

- UI updates immediately through local tombstones.
- Source-side delete failures are visible but do not resurrect hidden rows.
- Audit-only purge can minimize raw body/artifacts while retaining operator
  history.
- Intelligence can be extended for future sources without changing Messages UI.

## Phase 5: Cross-View Use

Use the same projection for operational awareness, map context, needs tracking,
operator history, and future protocol adapters.

- Map reads geo-capable projected messages.
- Attention queue reads actionable/operator-attention rows.
- Compose handoff uses source refs and routing metadata.
- Operator history links messages, reports, NCS activity, and purge audit.

Acceptance:

- Source adapters feed projections; views consume projections.
- Adding a new source family requires a projector and source contract, not a new
  bespoke inbox implementation.
