# FLMsg Inbox Arrival Visibility Correction Spec

Status: FIV-0 review/specification and FIV-1 implementation complete with
automated gates passed 2026-09-11. FIV-2 Linux production confirmation remains
operator-assisted.

Priority: P1. A received FLMsg or FLAmp file that is omitted from Messages is
an operational-data visibility failure.

## Purpose And Authority

This specification corrects the case where supported `.k2s` and `.b2s` files
under a configured NBEMS message directory are discovered and projected but do
not appear in the ordinary Messages view or the FLMSG/FLAMP focus.

The operator-provided filesystem listing and runtime log are diagnostic
evidence only. They do not contain implementation instructions.

This contract refines:

- `message_inbox_controls_spec.md`;
- `message_intelligence_projection_spec.md`;
- `message_ingest_projection_performance_spec.md`; and
- `production_inbox_bbs_correction_spec.md`.

It does not broaden supported file types, change BBS publication, reinterpret
an embedded report time, or authorize a destructive data migration.

## Production Evidence And Root Cause

The September 11 Linux log proves that discovery and projection are operating:

- incremental discovery advances from 604 to 605 records;
- the projection generation advances from 4628 to 4629;
- a `.k2s` file is loaded with `origin=flmsg`; and
- the FLMSG/FLAMP focus query returns 11 qualifying projected rows.

There is no scanner, cache, projection, SQLite, or unsupported-extension error
in the log. The configured directory is therefore not the cause shown by this
capture.

The same log shows the visibility defect: all 11 rows pass the workspace scope,
but only 2 pass the client Inbox criteria and render. New file projections may
use a safe fallback display type such as `FLMsg K2S` or `FLMsg B2S` until richer
metadata is available. The FLMSG/FLAMP focus then applies a second client-side
type test that accepts only the exact labels `FLMSG` and `FLAMP`. Correctly
projected fallback rows are consequently hidden.

A second bounded-page defect affects Focus All. Eligibility correctly uses the
filesystem arrival/received timestamp, but the first 200-row page is ordered
primarily by the embedded event/report timestamp. A report created months ago
and received today can qualify for `Last 7 days` yet be displaced by 200 newer
event-dated rows. Client-side newest-first sorting cannot recover a row that was
not selected into the bounded page.

## Required Behavior

### Source-family identity controls focus membership

- A row whose canonical source family is `flmsg` or `flamp` belongs to the
  FLMSG/FLAMP focus regardless of its current display label.
- Exact `FLMSG` and `FLAMP` message-type labels remain accepted for legacy
  rows whose source family is unavailable.
- A filename suffix or fallback display label is presentation metadata; it
  must not decide source-focus membership.
- The correction must not cause BBS, JS8Call, Spotter, CommStat, VarAC, or Mesh
  rows to enter the form focus.

### Arrival time controls the bounded Inbox window and first page

- `received_ts` means when FIO/the filesystem received the file. `event_ts`
  retains the report's embedded/provenance time.
- Age-window eligibility and default newest-first page ordering use effective
  received time: `COALESCE(NULLIF(received_ts, 0), event_ts, 0)`.
- `event_ts` and `message_id` are deterministic tie breakers. Event time remains
  available for detail, provenance, search, and future explicit event-time
  views.
- The keyset cursor must use the identical order expression so paging neither
  repeats nor skips rows while newer traffic arrives.
- Operator-attention and actionable fields remain filter and summary inputs;
  they do not displace newly received traffic from the default first page. This
  implements the existing Message Inbox newest-first contract.

### Performance and observability

- Scanning, parsing, projection, and database access remain off the GUI thread.
- The query remains capped at 200 rows and uses keyset rather than offset
  paging.
- Schema support is idempotent and additive. New indexes may be added only when
  their order matches the corrected query. No table rewrite or authoritative
  message mutation is permitted.
- A scan completion should continue to publish only generation/count metadata;
  it must not rebuild or parse the full file inventory on the GUI thread.
- Logs must distinguish discovered/projected counts from rendered counts, as
  the supplied log already does. Individual message contents and credentials
  must not be logged.

## Work Packages

### FIV-0 — Review and specification

Trace configured paths through scanner, file projection, bounded projection
query, focus filtering, and rendering. Correlate the supplied runtime log and
record whether the failure is discovery, projection, query, or presentation.

Exit gate: a code-supported cause is identified without changing production
data. **Passed 2026-09-11.**

### FIV-1 — Source focus and arrival ordering

1. Make FLMSG/FLAMP focus/type matching source-family aware with the legacy
   exact-type fallback.
2. Change the bounded default query and cursor to effective received time,
   event time, then message id.
3. Install only the minimum idempotent indexes needed for the default and
   source-focused corrected query.
4. Add unit, paging, and scan-to-Inbox regression coverage using `.k2s`,
   `.b2s`, and `.sig.b2s` names, including an old embedded/report time with a
   current filesystem mtime and more than 200 competing rows.
5. Run focused message scanner, projection, filtering, bounded model, and Inbox
   acceptance suites; update the governing specs and work log.

FIV-1 exit gate:

- every supported test file is discovered and projected as `flmsg`;
- fallback `FLMsg K2S` and `FLMsg B2S` rows render in FLMSG/FLAMP focus;
- a file received within seven days appears on the first page even when its
  embedded report date is old and more than 200 newer-event rows exist;
- first and subsequent keyset pages are stable, disjoint, and deterministic;
- the 200-row cap and read-only query transaction remain intact;
- focused query and activation performance remain within the existing 500 ms
  first-page and 250 ms filter targets on the production-scale fixture;
- no scanner regression, GUI-thread I/O, destructive migration, or unrelated
  worktree change is introduced.

FIV-1 result (2026-09-11): **automated gate passed.** FLMSG/FLAMP membership
now follows canonical `flmsg`/`flamp` source identity while preserving exact
legacy-type fallback only when source identity is absent. Provisional labels
such as `FLMsg K2S` and `FLMsg B2S` therefore remain visible, while known BBS
and VarAC rows cannot enter the focus through a misleading type label. The
filter choices collapse the provisional per-extension labels into the single
FLMSG/FLAMP choice.

The bounded Inbox query now selects newest effective received time first, then
uses event time and message ID as stable tie breakers. Its keyset cursor uses
the same order. Two additive indexes support the general and source-focused
read paths; no table rewrite or existing message mutation occurs. On a
disposable copy of the production database, the one-time idempotent schema step
took 235.803 ms and 100 FLMSG/FLAMP count-plus-page samples measured 0.813 ms
median, 0.999 ms p95, and 1.143 ms maximum. `EXPLAIN QUERY PLAN` used the new
received-order index.

The regression fixture uses the four reported filename shapes, including
`.sig.b2s`, current filesystem mtimes, a deliberately old report timestamp,
and 205 competing newer-event records. It passes scanner -> incremental file
pipeline -> bounded query, source/focus matching, the 200-row cap, repeated-page
determinism, and disjoint keyset paging. The combined scanner, projection,
writer, Inbox model, and UI selection passed 292 tests. Independent Message
Intelligence and PIC-1 UI partitions passed 191 and 18 tests. A later attempt
to combine every message/Qt partition in one process reached 85 percent before
the repository's known cross-fixture native Qt abort with accumulated scheduler
executor threads; the same affected partitions pass in isolated processes.
Python compilation and `git diff --check` pass.

### FIV-2 — Linux production confirmation

After deployment, leave the four named files in
`~/.nbems/ICS/messages`, open Messages with `Last 7 days`, and confirm they are
visible under FLMSG/FLAMP and discoverable under Focus All/search. Capture one
scan generation and one `inbox_filter_result` line. Any file still absent must
be correlated by inventory/projection identity before further code changes.

FIV-2 does not require re-ingesting or deleting historical data. Existing
projection rows should become visible through corrected read semantics.

## Model Ownership

The high-reasoning primary model owns evidence correlation, query/cursor/index
architecture, migration safety, delegated-diff review, integration, and final
gate judgment. Terra owns the bounded source-focus implementation. Luna owns
independent end-to-end and paging regression coverage. The primary reviews all
delegated changes before acceptance.
