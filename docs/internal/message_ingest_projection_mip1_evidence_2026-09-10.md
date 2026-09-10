# Message Ingest Projection MIP-1 Evidence

Date: 2026-09-10

Status: exit gate passed

## Delivered

- Projection schema version 3 adds durable dirty-work, source-state, and
  projection-generation tables and indexes through the existing startup-owned,
  additive migration boundary.
- Low-level source, message, reference, artifact, delete, and checkpoint row
  helpers no longer run schema assurance.
- Ops focus schema is included in startup projection migration, and the hot
  per-message index path no longer introspects or creates schema.
- `ProjectionBundleWriter` owns immutable atomic bundles, one serialized daemon
  lane and long-lived runtime SQLite connection, differential component writes,
  source deduplication, affected-only Ops indexing, 100-bundle and 50 ms
  transaction boundaries, generation advancement, bounded busy retry,
  cancellation, backpressure, and bounded shutdown.
- A process-wide registry returns one writer for each normalized database path.
- Reprojection preserves operator-read state when stale source input still says
  new/unread, in addition to the existing pinned, archived, and deleted
  precedence.

The migration is additive. It does not delete or reinterpret native messages,
files, BBS mappings, Spotter rules, FLAMP evidence, or operator state.

## Gate Evidence

The focused MIP-0/MIP-1 and projection/Ops integration set passes 56 tests. It
includes atomic rollback, the 100-bundle transaction cap, identical-bundle
zero-write behavior, changed-component indexing, source deduplication, bounded
busy retry with deferred identities, cancellation before a partial bundle,
generation advancement, startup migration idempotence, and a process-wide
single-writer registry.

Commands:

```text
./.venv/bin/python -m pytest -q tests/test_message_projection_writer.py tests/test_message_ingest_mip0_characterization.py
./.venv/bin/python -m pytest -q tests/test_message_projection_store.py tests/test_message_source_projectors.py tests/test_message_projection_projector.py tests/test_ops_focus.py
```

Results: 19 passed and 37 passed. Python compilation and `git diff --check`
pass.

## Ownership

- High-reasoning primary model: schema/migration and concurrency contract,
  operator-state preservation, writer-registry integration, busy-open handling,
  delegated-diff review, and gate decision.
- `gpt-5.6-terra` high: isolated serialized/differential writer implementation.
- `gpt-5.6-luna` high: schema-boundary, atomicity, batching, zero-write,
  deduplication, and registry tests.

MIP-2 may now replace whole-window source projection with durable incremental
adapters. MIP-1 does not activate old and new projector ownership concurrently.
