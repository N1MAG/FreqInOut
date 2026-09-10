# MIP-4 Bounded Query And Read-only UI Evidence

Date: 2026-09-10

Status: exit gate passed

## Delivered

- Inbox projection pages are keyset-paginated and hard-capped at 200 rows.
- Page rows, the optional total, and the committed projection generation are
  read from one explicit read-only SQLite snapshot.
- Source, multi-group, status, sender, recipient, message type, text, and both
  recent/older age boundaries are applied before the query row limit.
- Messages runs projection/reference reads on a dedicated worker, accepts only
  the latest request/generation, and updates its bounded model incrementally.
- Visible invalidations coalesce within 500 ms. Hidden or inactive views perform
  no table query/render and collapse changes into one activation refresh.
- Normal and forced Inbox refreshes never start the legacy retained-history row
  builder. Deep rebuild remains an explicit MIP-5 operation.
- UI list/get paths are read-only and perform no schema repair or primary-radio
  normalization. Station Control Bar profiles are cached outside repaint.
- Settings Save reconstructs SOP only while SOP is active; otherwise it marks
  that surface dirty for activation.
- Ops Center Traffic by Group uses a scalar SQL aggregate, preserving exact
  counts above 200 without message-body materialization. Actionable traffic uses
  a bounded attention query instead of the former 20,000-row load.

## Ownership

- High-reasoning primary model: architecture, query/filter semantics, Qt worker
  integration, command-bar/SOP boundaries, review, and final gate.
- `gpt-5.6-terra` high: read model, generation snapshot, group-volume aggregate,
  ControlFreq cutover, and focused core tests.
- `gpt-5.6-luna` high: bounded-model and asynchronous UI acceptance tests.

## Acceptance Evidence

The core projection/source/file/Ops/traffic partition passes 150 tests in 18.75
seconds. The clean Qt Messages, Station Control Bar, Settings/SOP, responsive
layout, and shell partition passes 361 tests in 1.89 seconds. Python compilation
and `git diff --check` pass.

A combined native Qt process reproduced the repository's previously documented
cumulative test teardown abort after more than 400 successful tests. The same
tests pass in clean partitions; this is not a MIP-4 product regression.

No production database or authoritative source was modified. Schema additions
are indexes installed only by the startup migration owner.
