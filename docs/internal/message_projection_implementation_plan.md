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

Implement source-specific projectors that populate the projection spine without
changing the UI yet.

- JS8Call projector: inbox rows and raw directed traffic.
- FIOSpotter projector: decoded forms, report keys, Expect/auth metadata.
- CommStat projector: artifact keys, status, scope, reach, source refs.
- SitRep projector: fused event rows and report keys.
- VarAC projector: messages, VMail, BBS links, callsign stats references.
- FLMsg/FLAmp projector: files, forms, FLAmp Q IDs, block/transfer state.
- BBS projector: live/archive mailbox entries with source-side capabilities.

Acceptance:

- Projectors are idempotent by external ref and content hash.
- Checkpoints avoid full rescans when no source changed.
- Enrichment jobs rerun only when content hash or intelligence version changes.
- Existing source tables/files remain authoritative and are not deleted.

## Phase 3: Message UI Read Path

Move the Messages tab from source-list row building to projection queries.

- Query `message_projection` for the default inbox.
- Lazy-load raw bodies/artifacts by `message_id` for detail panes.
- Use source refs for open-source, mark-read, delete, archive, and compose
  handoff.
- Keep bounded live views, with explicit history/search controls for older
  traffic.

Acceptance:

- Opening Messages does not parse external records.
- Default Messages query returns quickly with 100k projected rows.
- Filtering by source, group, status, severity, search, and focus uses indexed
  projection fields.
- CommStat/SitRep intelligence remains visible without per-row recompute.

## Phase 4: Intelligence And Purge Workers

Move rich intelligence and destructive source actions into background services.

- Materialize topics, entities, geography, severity, actionable state, and
  recommended actions.
- Preserve operator overrides and re-enrichment versioning.
- Process delete queue in source-capability-gated batches.
- Record external delete/archive results in audit rows.

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
