# Message Ingest Projection MIP-3 Evidence

Date: 2026-09-10

Status: exit gate passed

## Delivered

- Message-file discovery now returns an immutable delta containing added or
  changed, removed, unchanged, and replaced records without a second file walk.
- An unchanged 551-file snapshot reuses directory generations and performs no
  per-file stat, read, hash, parse, projection, or cache write.
- The off-UI file pipeline prepares only changed records, submits atomic
  message/reference/artifact bundles to the shared serialized writer in batches
  of at most 100, and tombstones removed or replaced external versions without
  deleting source files.
- The first run after the additive MIP-3 migration performs bounded catch-up
  from the current scanner snapshot even when the legacy GUI cache reports no
  delta. Later unchanged runs are read-only.
- File inventory and scanner-generation state are persisted off the Qt event
  thread in additive derived-state tables.
- Arbitrary POSIX filename bytes use a reversible opaque identity and a
  printable escaped display spelling, preventing invalid Unicode surrogates
  from failing SQLite binding or repeating a batch.
- The active scanner worker owns discovery, projection preparation, writer
  waiting, and cache persistence. Its Qt completion callback only swaps the
  snapshot, records generation state, invalidates the read model, and updates a
  small status string. It no longer parses files, writes cache/metadata, runs
  BBS housekeeping, projects observations, refreshes VarAC, verifies
  signatures, or reconstructs the message table.
- Existing compatibility helpers remain available for explicit deep-rebuild
  and legacy tests, but the ordinary file-refresh path no longer calls the
  legacy all-record file projector.

All schema changes are additive and derived-state-only. No message file, native
source row, operator state, BBS mapping, Spotter rule, or FLAMP evidence is
deleted or rewritten by migration.

## Gate Evidence

The integrated MIP-0 through MIP-3 projection set passes 320 tests. It includes
the production-shaped 551-file fixture, exact add/change/remove deltas, first-run
catch-up, unchanged zero-DML behavior, atomic artifact/reference commits,
surrogate-safe identities and display, static and dynamic Qt completion-boundary
checks, native adapter regressions, queue restart/collision cases, and existing
message-intelligence behavior.

Command:

```text
./.venv/bin/python -m pytest -q tests/test_message_projection_mip2_regressions.py tests/test_message_projection_coordinator.py tests/test_message_projection_queue.py tests/test_message_projection_writer.py tests/test_message_projection_store.py tests/test_message_source_projectors.py tests/test_ops_focus.py tests/test_message_ingest_mip3_file_pipeline.py tests/test_message_file_projection_pipeline_core.py tests/test_message_intelligence.py tests/test_message_ingest_mip0_characterization.py tests/test_ingest_source_model.py
```

Result: 320 passed in 17.71 seconds. Python compilation and `git diff --check`
also pass.

## Ownership

- High-reasoning primary model: file-delta and writer integration architecture,
  migration/catch-up semantics, Qt cutover, delegated-diff review, compatibility
  review, and exit-gate decision.
- `gpt-5.6-terra` high: file-path/delta core, bounded projection pipeline,
  inventory persistence, exact bundle reuse, and focused implementation tests.
- `gpt-5.6-luna` high: 551-file unchanged-work, add/change/remove,
  malformed-path, atomic bundle, and static/dynamic UI-boundary acceptance tests.

MIP-4 may now replace foreground list/count/model work with bounded indexed
queries and generation-driven invalidation. MIP-3 does not authorize an
implicit deep rebuild.
