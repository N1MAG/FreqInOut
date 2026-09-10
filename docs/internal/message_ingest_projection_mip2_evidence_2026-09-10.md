# Message Ingest Projection MIP-2 Evidence

Date: 2026-09-10

Status: exit gate passed

## Delivered

- Native JS8, FIO Spotter, VarAC, SitRep, and CommStat changes enter one
  durable, coalescing, lease-based dirty queue through source-transaction
  triggers and bounded watermark reconciliation.
- Queue identity includes the actual projection source, external kind, and
  external key. Duplicate VarAC row IDs or GUIDs on different endpoints remain
  distinct, and deletion is scoped to the owning source reference.
- VarAC reconciliation uses SQLite `rowid` rather than its endpoint-local `id`;
  projector/classifier changes reset a persisted watermark and resume in
  bounded batches.
- CommStat deletion markers are first-class incremental events. Removing a
  marker requeues the underlying artifact for projection.
- Late-created JS8 and Spotter tables install their triggers once through their
  schema lifecycle seam; no per-message schema assurance was introduced.
- Targeted adapters reuse the existing projector builders and query at most 100
  named identities per source. The active Inbox native worker now runs bounded
  coordinator cycles and no longer calls the 5,000-row legacy projector.
- Differential writes compare the complete persisted projection semantics,
  ignore volatile source-ingest timestamps, preserve operator-owned state, and
  index the post-upsert persisted row in Ops focus.
- Delete work uses the same bounded writer lane, source-scoped lookup,
  backpressure, busy retry, atomic dirty completion, and generation update.

All persistence changes are additive and derived-state-only. No native message,
operator state, file, BBS mapping, Spotter rule, or FLAMP evidence is deleted or
rewritten by migration.

## Gate Evidence

The focused MIP-2 integration set passes 65 tests. It covers unchanged
zero-DML reconciliation, one-row targeted projection, a 500-message burst in
five bounded cycles, forced-restart lease recovery without loss or duplicate
projection, version-reset resume, source-scoped VarAC collisions and deletion,
late source-table creation, CommStat marker deletion, full-field source
correction, read-state/Ops consistency, queue coalescing, writer atomicity, and
legacy projector semantics.

Command:

```text
./.venv/bin/python -m pytest -q tests/test_message_projection_mip2_regressions.py tests/test_message_projection_coordinator.py tests/test_message_projection_queue.py tests/test_message_projection_writer.py tests/test_message_projection_store.py tests/test_message_source_projectors.py tests/test_ops_focus.py
```

Result: 65 passed. Python compilation, Ruff checks for the changed core/test
modules, and `git diff --check` pass.

## Ownership

- High-reasoning primary model: durable identity/watermark architecture,
  source-scoped deletion, trigger lifecycle, coordinator cutover, concurrency,
  migration safety, delegated-diff review, and exit-gate decision.
- `gpt-5.6-terra` high: exact targeted-projector seam, independent concurrency
  audit, complete semantic-diff implementation, persisted-row Ops indexing, and
  focused regression tests.
- `gpt-5.6-luna` high: queue, burst/restart/version, source-collision,
  lifecycle, deletion-marker, and state-consistency acceptance tests.

MIP-3 may now replace the file scan/projection completion path. MIP-2 does not
authorize an implicit deep rebuild.
