# Message Ingest, Projection, And UI Responsiveness Specification

Status: implementation complete; MIP-0 through MIP-5 implementation exit gates
passed; the September 10 production responsiveness remediation implementation
gate passed; Linux production confirmation remains external release qualification

Date: 2026-09-10

Scope: message-source change detection, normalized message/reference/artifact
writes, file discovery, projection catch-up, Inbox query/model refresh, FIO
Spotter/Expect independence, SQLite concurrency, lifecycle, telemetry, migration,
and performance qualification

## Purpose And Authority

This specification turns the September 10 Linux production traces into a
bounded message-processing architecture. It supplements:

- `production_reliability_and_workflow_remediation_spec.md`;
- `message_intelligence_projection_spec.md`;
- `message_inbox_controls_spec.md`; and
- `multirig_product_ui_contract.md`.

For message ingest, projection cadence, database-write ownership, UI refresh,
startup catch-up, and the performance gates defined here, this document is the
more specific authority. Existing message meaning, routing, authorization,
retention, deletion, and FIO Spotter protocol contracts remain authoritative.

The intended operator experience is simple:

- a newly received message normally becomes visible within one-half second;
- bursts arrive progressively without freezing the application;
- opening Messages, changing a filter, or resizing never scans or rewrites
  retained history;
- no work is performed merely because a timer fired when nothing changed;
- FIO Spotter Expect replies remain prompt even while Inbox catch-up is active;
- a source, radio, or database delay cannot hide the Station Control Bar or
  delay scheduler work for unrelated endpoints; and
- derived indexes can always be rebuilt without losing source evidence.

## Production Evidence And Confirmed Failure Modes

The September 10 artifacts contain one FIO launch sampled at different times.
The shorter FIO and performance logs are prefixes of the longer copies. Both UI
hang reports identify process 58119.

Observed timings:

- first usable shell: 43.426 seconds;
- startup complete: 44.325 seconds;
- main-window construction: 31.675 seconds;
- database initialization: 7.270 seconds;
- Settings construction: 5.846 seconds;
- first native message projection: 152.752 seconds for 11,957 source rows;
- later native message projection: 267.269 seconds for 10,583 source rows;
- initial file-scan completion handler: 9.229 seconds;
- unchanged 551-file scan totals: 5.934 to 7.567 seconds;
- Station Control Bar callbacks: 23 samples, 954.7 ms mean, 2.628 seconds max;
- Settings save: 7.033 seconds; and
- UI watchdog stalls: message completion, SOP reconstruction after Settings
  save, and a Station Control Bar profile read that performed database repair.

The source counts demonstrate whole-window replay. A changed source fingerprint
reprojects as many as 5,000 CommStat rows and 5,000 SitRep rows. Each projected
row currently upserts its source, message, external reference, optional artifact,
and Ops index. Each individual store upsert also re-runs projection-schema
assurance. Long source transactions hold SQLite write access while Python row
construction competes for the interpreter, causing foreground delays and
`database is locked` errors in Mesh and operator lookups.

The file path also has two defects: unchanged scans still perform foreground
completion work, and an invalid Unicode surrogate can repeatedly fail native
file projection instead of being represented safely as source evidence.

## Terminology

- **Source record:** authoritative data in a native FIO source table or an
  observed file owned by another application.
- **Projection:** the normalized, rebuildable message row used for bounded
  queries, intelligence, and presentation.
- **Reference:** the link from a projection to its native source identity.
- **Artifact:** a file, FLAMP transfer/block state, attachment, or other material
  associated with a projected message.
- **Bundle:** one projection and the references/artifacts that must become
  visible atomically with it.
- **Dirty item:** durable notice that one native identity needs projection,
  reclassification, deletion reconciliation, or read-state reconciliation.
- **Watermark:** per-source progress proving which native changes have been
  considered successfully.
- **Catch-up:** bounded processing of dirty items after downtime, migration, or
  a projector-version change.
- **Deep rebuild:** explicit reconstruction of derived projection state. It is
  never an ordinary tab-open, timer, or startup action.

## Non-Negotiable Invariants

1. Native source evidence is authoritative. Projection is derived and
   rebuildable.
2. A message bundle is atomic: its message row and required references/artifacts
   commit together or none commit.
3. UI callbacks never parse retained bodies, traverse source directories, run
   schema assurance, wait for a database writer, or project history.
4. Opening or refreshing a view never causes whole-history projection.
5. A source change only projects identities affected by that change. A bounded
   reconciliation may discover missed identities, but must not rewrite unchanged
   rows.
6. An unchanged timer tick performs a watermark or directory-generation check
   only and causes no message/ref/artifact writes.
7. There is one serialized projection writer per FIO message database. Multiple
   producers may enqueue work, but they do not compete as SQLite writers.
8. The writer never owns source I/O or expensive parsing while a write
   transaction is open.
9. Existing pinned, archived, deleted, read, and operator-managed states survive
   reprojection unless the corresponding explicit action changed them.
10. FIO Spotter Expect evaluation and response dispatch never wait for Inbox
    projection, file-table painting, or historical catch-up.
11. The scheduler, Station Control Bar, Mesh, BBS, and roster services never wait
    synchronously for message projection.
12. Shutdown is bounded. Uncommitted derived work remains discoverable through
    its durable dirty record or source watermark after restart.

## Ownership And Processing Architecture

### Source adapters and durable change identity

Each native source adapter owns detection of committed source changes. Supported
families initially include JS8Call, FIO Spotter, VarAC, SitRep, CommStat, FLMsg,
FLAMP, BBS artifacts, and Mesh messages eligible for Inbox presentation.

After committing authoritative source state, the adapter records a durable dirty
identity. If an existing legacy writer cannot call the adapter directly, an
additive table trigger or a bounded source-specific reconciler must create the
same dirty identity. Polling may discover work, but must not perform projection.

A dirty identity contains at least:

- source family and stable source id;
- external kind and stable external key;
- operation (`upsert`, `delete`, `read_state`, `reclassify`);
- source version or updated timestamp;
- projector/classifier version and priority;
- first- and last-observed timestamps; and
- retry/error state without message content or credentials.

The durable queue has one logical active row per source identity. Repeated
changes coalesce to the newest source version. Coalescing removes redundant work,
not evidence: authoritative data remains in its source table or file.

### Projection preparation worker

One bounded preparation worker reads dirty identities and builds immutable SQL
bundles outside a write transaction. The worker:

- reads only native rows named by the dirty batch;
- canonicalizes source text and unsafe filesystem names;
- performs intelligence only when relevant input or version changed;
- computes stable ids and content hashes;
- compares existing hashes in one bounded query;
- omits materially unchanged rows;
- prepares identical source metadata once per source; and
- hands bundles to the writer without Qt objects.

Preparation uses bounded Python slices. The normal maximum is 100 bundles or 10
ms of uninterrupted CPU preparation, whichever occurs first. Remaining work is
requeued so the main interpreter thread can service the UI. A future process
worker may handle genuinely CPU-heavy parsers, but cannot own SQLite writes or
change bundle semantics.

### Serialized projection writer

One long-lived writer services the normalized message database. It is not a new
thread per refresh. It accepts prepared batches and performs only:

- changed source metadata upserts;
- changed message projection upserts;
- reference and artifact diffs;
- affected Ops/entity-index updates;
- dirty-item completion; and
- watermark advancement.

Schema creation, column assurance, and index assurance happen once under the
startup migration owner. Row-level store helpers must not call schema assurance.
Reusable helpers may assert a connection's schema version in tests or debug
mode, but that assertion cannot execute DDL or table introspection per row.

Source metadata is upserted once when its signature changes, not once per
message. Message SQL uses content hashes to avoid updating unchanged rows and
`projected_utc`. Ops/entity indexing runs only when indexed fields changed.

### Transaction boundary

The normal transaction contains at most 100 complete message bundles or 50 ms
of writer time, whichever boundary is reached first. The writer may reduce the
row cap dynamically to preserve the time budget. It never splits one bundle
across transactions.

After a batch commits, the writer yields before beginning the next batch. Under
SQLite contention it rolls back cleanly and reschedules with bounded jittered
backoff beginning near 50 ms and capped at two seconds. UI code never waits for
that retry. Persistent failure marks the source delayed and preserves the dirty
item for recovery. WAL mode and busy timeout are implementation details, not
substitutes for short transactions.

## Cadence And Backpressure

### Normal traffic

- The first dirty item starts a 250 ms coalescing window.
- Reaching 100 ready bundles closes the window early and dispatches immediately.
- Subsequent batches run only while work exists.
- A priority item may bypass debounce but still uses the serialized writer and
  atomic bundle contract.
- No periodic full projection is permitted.

The 250 ms value may be tuned between 100 and 500 ms from measured evidence. It
is an internal performance setting, not routine operator configuration.

### Inbox presentation

Writer commits publish a small invalidation containing affected source/focus
families, counts, newest event timestamp, and projection generation. They do not
send complete message bodies through a Qt signal.

- While Messages is visible, invalidations coalesce into at most one model query
  every 500 ms.
- While Messages is hidden, no table is rendered. Summary invalidations may
  coalesce for up to two seconds.
- Returning to Messages reads the latest bounded page and counts; it does not
  replay every missed UI invalidation.
- Filter, age, source, group, intelligence, and search changes query the indexed
  projection and never invoke source scanning.
- The first page is capped at 200 rows. Additional history uses explicit paging
  or virtualized fetch.
- A model diff updates changed rows and counts. Complete widget reconstruction
  is reserved for a structural column or theme change.

### Idle reconciliation

Every 30–60 seconds, a low-cost coordinator may compare source watermarks,
change-journal generations, and watched-directory generations. When all match,
it performs no source-row fetch and no write. Cadence uses jitter so it does not
align with scheduler, Mesh, BBS, and health timers.

A bounded reconciliation repairs missed notifications by selecting rows newer
than the source watermark or rows named in the native change journal. Count/max
fingerprints are diagnostic hints only; a fingerprint change must not cause the
newest 5,000 rows to be rewritten.

### Backpressure and priority

Queue growth is bounded in memory. Durable identities remain in SQLite, so
memory pressure can discard only cached queue entries and reload them later.

Priorities are:

1. operator action state needed for a visible row;
2. newly received actionable/directed message bundles;
3. ordinary new traffic;
4. read/delete/archive and source reconciliation; and
5. historical/version catch-up.

Priority changes ordering only. It cannot weaken atomicity, authorization,
deletion, or retention. Older work for one identity is superseded by its newest
durable source version.

## Startup And Historical Catch-Up

Startup performs, in order:

1. the idempotent additive schema migration;
2. loading persisted source watermarks and queue summary;
3. presenting the usable shell from cached summaries;
4. starting source listeners and the projection coordinator; and
5. resuming bounded dirty/catch-up batches after the shell is usable.

Startup does not synchronously scan message directories, project native source
windows, verify signatures, or populate an unbounded table. Messages may show a
non-blocking `Updating message index` state with cached results and bounded
progress.

A projector/classifier version change marks only rows with an older stored
version for resumable reclassification. Progress is persisted; restart does not
restart the historical window.

An explicit deep rebuild is a maintenance action with preview, estimated row
count, progress, cancellation, and a resumable checkpoint. It cannot begin
implicitly when the operator opens Messages or FIO Spotter.

## File Discovery And Artifact Handling

File discovery separates directory detection from parsing and projection.

- Prefer filesystem notifications where reliable; retain a bounded safety poll.
- Cache normalized directory identity and directory generation/mtime.
- An unchanged directory produces no per-file stat/hash/parse pass.
- A changed directory enumerates only that directory and diffs stable identity,
  size, and mtime against the cache.
- New/changed files are parsed off the UI thread and produce dirty identities.
- Removed files produce reconciliation work; they are not silently republished
  or erased from audit history.
- Signature verification is separate and cannot delay initial display.
- Invalid byte sequences and surrogate paths use a reversible escaped display
  and provenance representation. One malformed name cannot fail the batch or
  trigger an endless retry.

The file worker publishes an immutable delta. The UI completion callback may
swap a cache reference and schedule presentation invalidation; it must not save
scan caches, apply BBS sweeper rules, project observations, refresh VarAC, or
repopulate the message table synchronously.

## FIO Spotter And Expect Fast Path

Expect handling is operational traffic with its own bounded path:

1. receive and parse the query;
2. evaluate authorization from cached/indexed policy data;
3. evaluate the requested capability from its authoritative indexed source;
4. durably claim duplicate/cooldown ownership;
5. dispatch through the receiving JS8 endpoint; and
6. append audit/source evidence.

It does not call the general projector, walk FLAMP directories, refresh Inbox,
or wait for historical reclassification. FLAMP Q correctness continues to use
the dedicated freshness/reconciliation contract. A projection backlog cannot
turn a known answer into `NO` or delay a response behind historical messages.

Expect receive/reply evidence enters normal projection after the operational
decision. UI visibility is eventually consistent without being in the
response-critical path.

## Operator Actions And State Preservation

Read, delete, archive, and pin actions update the visible model immediately with
a pending state and enqueue an atomic source/projection action. Success confirms
the row; failure restores the prior state with clear feedback. An action cannot
block Qt on a busy database.

Projection replay preserves operator-owned state. Native read state may advance
derived state where its source contract permits, but replay cannot unarchive,
undelete, unpin, or mark unread merely because a projector supplied defaults.

Source deletion, projection deletion, BBS publication removal, and physical-file
deletion remain distinct. This performance design grants no new deletion scope.

## Database And Concurrency Contract

- The serialized writer solely owns projection-table mutations at runtime.
- Other services consume committed snapshots or their own native stores and do
  not borrow the writer synchronously.
- `list` and `get` APIs are read-only; they cannot repair, update, or commit.
- Repair runs only during migration or explicit configuration mutation.
- Connections are thread-owned and never shared across workers.
- Prepared work carries a generation; late retired-source/configuration results
  cannot overwrite newer state.
- Message work cannot hold scheduler endpoint lanes or central RF coordination.
- Shutdown stops acceptance, persists outstanding identities, cancels unstarted
  preparation, waits only for the current short transaction, and remains within
  the application shutdown budget.

## Failure And Recovery

- **Crash after source commit:** dirty state shares the source transaction where
  possible; otherwise watermark reconciliation discovers the change.
- **Crash during projection:** the bundle transaction rolls back and dirty work
  remains eligible. Prior committed batches are not replayed without cause.
- **Busy database:** report bounded lock-wait metrics, back off, and retry without
  blocking UI or unrelated endpoints.
- **Malformed content:** quarantine one identity with safe provenance; continue
  the batch and retry only when its source version changes.
- **Missing/removable source:** retain last known evidence as stale. Temporary
  unavailability cannot manufacture deletion or a negative Expect answer.
- **Repeated failure:** emit one throttled source-level health event, never an
  error per row or tight retry loop.

## Observability

Required aggregated spans/counters include:

- `messages.dirty_detect`: source, operation, new/coalesced counts;
- `messages.prepare_batch`: requested/prepared/unchanged/quarantined and CPU/wall
  time;
- `messages.write_batch`: bundles, SQL rows, commit time, lock-wait time;
- `messages.projection_lag`: oldest/newest dirty age and queue depth;
- `messages.reconcile_source`: watermark delta and discovered identities;
- `messages.file_discovery`: changed directories/files and unchanged shortcut;
- `messages.model_query`: filters, bounded rows, count-query time;
- `messages.model_apply`: inserted/updated/removed rows and UI time;
- `messages.expect_fast_path`: evaluation/claim/dispatch time without content;
  and
- shutdown queued/persisted/committed/cancelled/survivor counts.

Metrics aggregate by source and interval. Bodies, callsigns, credentials,
filesystem secrets, and per-message identities are excluded from normal logs.

`perf_metrics.log` uses bounded rotation, recommended at 5 MiB with five backups,
and a long-lived buffered/queued writer. Emission cannot open and close a file
for every span. Routine scheduler no-op and unchanged source activity is debug or
aggregated. The watchdog reads cache-only queue/writer diagnostics and performs
no SQLite or source-file access while dumping state.

## Performance Budgets

Budgets are measured on the Linux production-sized database and repeated on
macOS. Unless stated otherwise, gates use p95 across at least 100 operations.

| Operation | Budget |
| --- | ---: |
| First usable shell, warm | <= 5 s |
| First usable shell, cold | <= 10 s |
| Visual click acknowledgement | <= 100 ms |
| Routine Qt callback | <= 50 ms |
| Cached Messages activation/filter | <= 250 ms |
| First bounded Messages page | <= 500 ms |
| New source commit to visible Inbox | <= 500 ms normal load |
| Prepare one normal 100-bundle batch | <= 25 ms p95 |
| Projection write transaction | <= 50 ms p95, <= 100 ms max |
| Unchanged source reconciliation | <= 25 ms per source |
| Unchanged file discovery | <= 250 ms |
| Station Control Bar cached render | <= 50 ms |
| Expect evaluation/claim before transport | preserve existing <= 10 ms core target |
| Normal shutdown | <= 3 s |

Additional gates:

- no watchdog event during startup, a 500-message burst, historical catch-up,
  filtering, Settings save, or shutdown;
- one changed row does not rewrite a fixed 5,000-row source window;
- unchanged activation produces zero projection writes;
- 500 incoming messages become queryable within five seconds while UI heartbeat
  latency remains below 250 ms;
- 12,000-row legacy catch-up completes without database-lock errors, without a
  transaction above 100 ms, and without delaying scheduler commands;
- idle FIO performs no repeated full scans or projection writes;
- durable work survives forced termination with bounded memory;
- qualification samples process CPU. Sustained single-core saturation beyond
  ten seconds fails unless the work is an explicit deep rebuild and UI/scheduler
  budgets remain satisfied; and
- threads, descriptors, child processes, queues, and RSS stabilize in the
  30-minute integrated soak.

## Additive Persistence Design

Exact names may be refined, but the ownership model requires:

### `message_projection_dirty`

- composite stable source identity;
- requested operation and priority;
- first/last observed timestamps and source/projector versions;
- attempt count, retry time, and bounded error code;
- lease owner/expiry for crash-safe claiming; and
- a unique constraint supporting latest-state coalescing.

### `message_projection_source_state`

- source id/family;
- high-water id/timestamp or native change generation;
- projector/classifier version;
- last successful reconciliation;
- last known source availability/freshness; and
- diagnostic counts without message contents.

Existing projection, external-reference, artifact, and checkpoint tables remain.
Indexes are additive and idempotent. Existing checkpoints may seed new state but
cannot prove row-level completeness without bounded reconciliation.

Migration is additive and non-destructive. It does not delete or rewrite native
messages, files, operator state, BBS mappings, Spotter rules, or FLAMP Q evidence.
Rollback may disable the coordinator while leaving queue/state tables in place.
Old and new projectors must never write concurrently.

## Implementation Packages And Exit Gates

### MIP-0 — Characterization and safety fixtures

Deliver a production-shaped fixture with at least 5,000 CommStat, 5,000 SitRep,
1,000 Spotter, JS8, VarAC, and 551 file records; measure current schema/SQL
amplification; and add deterministic UI-heartbeat, lock, crash/restart, and
malformed-path tests.

Exit gate: tests reproduce full replay, foreground completion, and read-side
database mutation without changing production behavior.

### MIP-1 — Schema lifecycle and atomic bundle writer

Deliver startup-owned migration, schema-free row helpers, differential bundle
SQL, source deduplication, the serialized bounded writer, and lock/cancellation/
crash tests.

Exit gate: atomicity and transaction budgets pass; one changed row produces one
bundle and zero per-row schema checks.

### MIP-2 — Durable queue and incremental source adapters

Deliver durable coalescing/source watermarks; JS8, Spotter, VarAC, SitRep, and
CommStat adapters; bounded missed-change reconciliation; versioned resumable
reclassification; and deletion/read-state reconciliation.

Exit gate: normal and burst matrices pass without 5,000-row replay or lost work
across forced restart.

### MIP-3 — File delta and artifact pipeline

Deliver changed-directory/file diffing, off-UI parsing/cache persistence,
surrogate-safe handling, atomic artifact/reference projection, and separation of
BBS/FLAMP/signature work from UI completion.

Exit gate: unchanged and changed-file budgets pass on the 551-file fixture;
malformed paths do not fail or repeat a batch.

### MIP-4 — Bounded query/model and read-only UI dependencies

Deliver indexed page/count queries, generation invalidations and model diffs,
visible/hidden coalescing, removal of repair from all list/get paths, cached
Station Control Bar inputs, and relevant-only lazy SOP Settings-save refresh.

Exit gate: tab/filter, command bar, Settings/SOP, compact/large text, and
light/dark budgets pass without a watchdog stall.

### MIP-5 — Startup, telemetry, and production qualification

Deliver post-shell catch-up, explicit deep rebuild, rotated/buffered metrics,
cache-only watchdog diagnostics, Linux/macOS CPU and lock evidence, and a
30-minute integrated scheduler/message/Expect soak.

Exit gate: every performance/lifecycle budget passes. The physical scheduler
hardware gate remains separate and is not weakened.

Implementation evidence: non-critical ingest and message catch-up start only
after the first usable shell; one application-owned maintenance lane performs
bounded catch-up and jittered reconciliation; explicit Message Index preview,
reset, and progress work runs off the Qt thread; telemetry is buffered and
rotated; and watchdog dumps consume only precomputed, credential-redacted
diagnostics. The automated macOS qualification, real 12,000-row catch-up, clean
Qt partitions, and 30-minute integrated soak pass. The development host cannot
measure the user's Linux compositor, filesystem, Bluetooth stack, production
database, or physical endpoints, so Linux production confirmation remains a
named external release-qualification observation rather than an invented local
result. Details are recorded in
`message_ingest_projection_mip5_evidence_2026-09-10.md`.

No package begins its successor before its gate passes. A migration that deletes,
rewrites, or reinterprets authoritative source data requires separate operator
approval.

## Required Test Matrix

At minimum, test:

- zero changes; one insert/update/delete/read-state change;
- repeated updates to one identity inside debounce;
- 500-message and mixed-source bursts;
- 12,000-row first catch-up and interrupted resume;
- projector/classifier version increments;
- busy database and crash before/after commit;
- unavailable, restored, malformed, and removed sources;
- invalid Unicode filename/body content;
- visible, hidden, minimized, and inactive UI states;
- filter changes during ingest and tab close during query;
- Settings save with SOP inactive and active;
- a hung radio/SDR while catch-up runs;
- Mesh retry and BBS reconciliation during projection;
- dynamic FLAMP Q evaluation with empty/saturated projection queues;
- startup followed immediately by Messages, Map, Spotter, and Settings;
- bounded shutdown with queued work; and
- restart proof that committed evidence was neither lost nor duplicated.

## September 10 Production Responsiveness Remediation

The later Linux production capture is a stricter requalification of MIP-5, not
a new feature slice. It showed that the original per-source reconciliation
bound was not a global cycle bound: JS8, Spotter, VarAC, SitRep, and CommStat
could each discover 100 identities in one cycle. A following cycle could then
discover more work before draining the durable queue. Preparing 100 complex
bundles at once also held the Python interpreter for seconds, and write batches
were observed taking 33 to 108 seconds while unrelated Qt callbacks waited.

The binding remediation is:

- durable dirty work is always drained before any new historical discovery;
- one reconciliation cycle may discover at most 100 identities across all
  native sources combined, not 100 per source;
- bundle preparation yields between 25-identity slices;
- writer CPU and transaction units are capped at 25 bundles while retaining the
  100-identity coordinator budget;
- catch-up cancellation propagates through reconciliation, preparation,
  writes, deletion, lease release, and nonblocking shutdown; and
- a cancelled unit remains represented by its durable dirty identity and is
  immediately reclaimable (or safely recoverable after its short lease).

The ordering intentionally favors bounded latency over maximum historical
throughput. A new source cursor is not advanced while older durable work exists.
FIO Spotter Expect evaluation remains outside this lane.

Automated requalification passes 134 message ingest/projection tests, including
the global discovery cap, monotonic queue drain, cancellation/lease recovery,
writer transaction slicing, restart, projection query, file pipeline, telemetry,
and UI-async contracts. Live Linux startup, backlog-drain CPU, message latency,
and idle confirmation remain external and must be measured before the production
release gate is closed.

## Definition Of Done

Work is complete only when normal ingest is incremental/event-driven; the UI
causes no source projection or database repair; bundles are atomic/differential;
unchanged checks cause no writes; Expect remains independent; Linux/macOS
startup, burst, idle, contention, and shutdown budgets pass; the September 10
failure shapes are regression tests; logs are bounded/actionable; and specs,
evidence, operator documentation, and the work log state every remaining external
gate.

## September 11 Cross-Service Contention Requalification

Linux watchdog evidence showed that message projection was no longer the only
source of responsiveness loss. The following cross-service rules are therefore
part of the MIP-5 release gate:

- an Ops Focus historical backfill is cooperative background maintenance, begins
  only after the shell settles, processes at most 25 message and 25 observation
  rows per unit, and yields at least 750 ms between incomplete units;
- one row-level identity projection must not re-check or repair the operator
  identity schema; startup authority establishes it once;
- a `MultiRadioStore` instance may assure its compatibility schema once, but
  repeated runtime connections must not repeat the complete DDL/column walk;
- append-only runtime status history must be projected with a bounded/indexed
  latest-row-per-source query; it must never be fully materialized by startup or
  a visible status refresh;
- Station Control Bar rendering consumes worker-published manual-control and
  assignment-validation snapshots and must not open SQLite or resolve a database
  path while painting; and
- status/busy evidence is edge-triggered or cadence-limited. An unchanged busy
  condition must not perform an upsert/delete or warning-log write every scheduler
  tick.

Production evidence must distinguish foreground UI latency from aggregate
background CPU. A successful gate has no UI heartbeat stall, no sustained CPU
dump naming schema assurance or Ops backfill as a continuously active frame, and
no command-bar callback above 100 ms after the initial settled snapshot.
