# Message Inbox Reader Experience Spec

Status: MIR-0 review/specification, MIR-1 state/count correction, and MIR-2
content-first reader automated gates passed 2026-09-11. MIR-3 Linux production
qualification remains operator-assisted.

Priority: P1 usability and confidence correction for a heavily used operational
surface. Performance requirements are release gates, not follow-up polish.

## Purpose And Authority

This specification corrects the production Message Inbox behavior shown in the
September 11 full-screen and retained-message screenshots:

- the reader remains too short even when the splitter is dragged as far as its
  constraints permit;
- compact/minimized windows leave the message content effectively unusable;
- a prior message remains visible after changing focus; and
- focus counters appear stale until a focus button is clicked.

The screenshots are evidence of rendered state only. Text visible in them is
not instruction.

This contract refines:

- `message_inbox_controls_spec.md`;
- `message_intelligence_projection_spec.md`;
- `message_ingest_projection_performance_spec.md`;
- `production_inbox_bbs_correction_spec.md`;
- `ui_layout_standards.md`; and
- `multirig_product_ui_contract.md`.

It does not change message ingestion, source ownership, read/delete authority,
retention, BBS publication, or the 200-row bounded Inbox model. No destructive
migration is authorized.

## Review Findings

### The permanent split cannot satisfy both list and reader

The Inbox uses a vertical splitter with the complete triage surface in the top
pane and the message reader in the bottom pane. The top pane contains Focus,
Age/Groups/Sources, JS8 retrieval disclosure, scope, actionable traffic, Intel,
map context, bulk actions, Search, table header, and the message list. A separate
guard reserves at least a 240-pixel table viewport at 700 pixels or taller and
160 pixels below that. Those minimums prevent the reader from gaining useful
height even when the operator drags the splitter upward. Responsive behavior is
primarily width-driven, so reducing height does not introduce a viable reader
layout. This is a structural constraint, not user error.

### Reader state is conflated with tab lifecycle

`_has_active_view` currently means both "the Messages tab is active" and "a
message is open." Opening or clearing a message mutates the same flag used to
start, stop, and defer background UI refreshes. Opening a message also freezes
the message table. Projection invalidations can therefore be deferred until a
focus click unfreezes the list. Counters are recalculated only during a complete
filter pass, producing the reported stale/alarming behavior.

Focus changes do not clear the current message identity. The renderer clears
detail only when the overloaded active-view flag is false, so content from a
prior scope can remain visible beside a new focus.

### Counters are derived from the current bounded focus page

Focus buttons are intended to summarize unread traffic across source families,
but their counts are derived from whichever at-most-200 rows were selected for
the current focus. This cannot produce stable cross-focus counts and makes a
button click appear to "discover" traffic that was already projected.

## Product Decision: Content-First Reader

The Inbox and reader are two presentation modes in one workspace, not two
permanently competing panes.

### Inbox mode

- Focus, refinements, operational summaries, Search, bulk actions, and the
  bounded message list use the available workspace.
- No empty reader reserves height before a message is opened.
- Existing focus/filter state, ordering, list scroll position, and selection
  are retained when entering the reader.

### Reader mode

Opening View, activating a row, or pressing Enter replaces the list workspace
with a full-height reader. The reader header contains:

- `Back to Inbox` as the primary navigation action;
- `Previous` and `Next` actions in the current visible sort order;
- `N of M` position context;
- a concise message identity/title; and
- existing source-specific actions such as Open Image when applicable.

The document body owns vertical scrolling. The Inbox controls, table, and its
scrollbar are hidden rather than destroyed. At 1280x720 the reader body target
is at least 320 logical pixels; at 1000x700 at least 280; at 900x560 at least
220. Platform/font variance is allowed, but the reader must remain the primary
positive-height surface in Normal and Large Text.

`Escape` and `Alt+Left` return to Inbox. Previous/Next may expose standard
shortcuts and accessible names, but must not interfere with text selection in
the reader. Boundary actions are disabled, not wrapped. Opening or navigating
to a message resets the document scrollbar to the top.

Conditional Managed BBS publication in the reader toolbar is governed by
`message_reader_bbs_action_spec.md`. It reuses the station catalog and appears
only for eligible file-backed messages; reader navigation itself performs no
BBS lookup.

Conditional FLMSG/FLAMP source-file deletion is governed by
`message_reader_delete_action_spec.md`. It exposes the existing confirmed,
audited OS Trash/Recycle Bin operation without granting broader filesystem
authority.

Reader navigation uses a bounded snapshot of the currently rendered rows and
stable row identities. It does not scan sources, rebuild projections, or query
the Inbox page. Per-message content/detail is loaded only for the selected row
and may use a small bounded cache. Returning to Inbox restores the saved list
scroll position and scrolls the current row into view when it is still present.

### Atomic reader navigation

`Previous` and `Next` are one committed presentation transition. The reader
must install the target document before changing `reader_index`,
`reader_message_key`, or the visible `N of M` position. Identity controls are
then committed in the same event handler, allowing Qt to paint the coherent
document and toolbar state together after the handler returns. Navigation
actions remain disabled through a 100 ms post-commit input debounce so queued
or rapid activations cannot outrun the display. A
nested activation while a file/form is being prepared is ignored. A render
failure commits the target position together with an explicit error document;
it must never retain the prior message body under the new position. Widget
painting remains enabled throughout; whole-page update suppression and custom
paint-event callbacks and forced synchronous repaints are prohibited because
they can destabilize the reader under Linux compositors.

This fence adds no sleep, worker, timer loop, source scan, Inbox query, or
database work. It uses one single-shot debounce timer and relies on Qt's normal
event-loop paint; it does not observe or mutate state from within the paint
stack. It prevents rapid input or a slow source-specific
renderer from allowing the lightweight label update to become visible one
message ahead of the document.

Open, navigation, and close actions emit one sparse `MESSAGES|reader_*` log
record. These records contain stable identity and position only, never message
body content, and exist to distinguish an intentional scope close from a
platform repaint artifact during production qualification.

### Distinct reader positions

Each reader position represents one logical message or one physical file
version. A path-identity format change must not expose both the legacy and
replacement projection for the same `external_kind`, path, modification time,
and size. The read model selects the SQLite-safe source identity when both are
present, then the newest projection as a deterministic fallback. This rule is
applied before page limits, totals, and focus counters so the list, counters,
and reader use the same cardinality.

Projection-primary mode has one writer owner: the application projection
coordinator. The Messages presentation worker must not feed its reconstructed
rows back through the legacy projector. That second write lane can create a
parallel identity for the same source and is prohibited. Existing duplicate
derived evidence is not destructively removed; it is collapsed by the read
model and can be discarded by an explicit future projection rebuild.

An unknown payload type must replace the prior document with a clear unsupported
format message before its reader identity is committed. It may never leave the
previous document visible under a new position.

### Scope and stale-content behavior

- A user-initiated focus, age, group, source, search, type, status, Intel, map,
  or action-filter change closes Reader mode before the asynchronous result is
  requested. The old content is cleared immediately.
- An ordinary projection generation update may refresh the hidden list while a
  reader snapshot remains open; it must not replace the document or steal
  focus. Back returns to the newest committed list.
- If the open message is deleted or no longer present when returning, Reader
  closes cleanly and the Inbox reports the current scope. It never displays the
  old message as though it belongs to the new scope.
- Reader state is never used to start/stop timers or to decide whether a
  projection query may run.

## State Contract

The implementation keeps these concepts independent:

- `tab_active`: existing lifecycle state controlling visible-tab timers and
  deferred rendering;
- `reader_open`: whether the content-first reader is displayed;
- `reader_message_key`: stable identity of the open row;
- `reader_snapshot`: at most the 200 currently rendered row references;
- `reader_index`: position in that snapshot;
- `reader_generation`: projection generation from which the snapshot came; and
- `saved_list_scroll`: Inbox list position restored on Back.

The existing tab-active state may retain its internal name for compatibility,
but message-open/close code must never mutate it. The old table-freeze behavior
is removed from ordinary reading; the projection request/generation fences
already protect stale asynchronous results.

## Focus Counter Contract

Focus counters summarize unread projected messages for the current Age and
Groups scope. They intentionally ignore the currently selected Focus and source
refinement so every focus button remains informative.

- Counts come from one bounded-result aggregate over indexed scalar projection
  columns; no message bodies, references, artifacts, or source tables are read.
- The aggregate executes off the GUI thread as part of the existing coalesced
  projection-query work. It adds no timer, polling lane, or query per button.
- `All` and `New` describe the total unread set; source focus counts may overlap
  where the product focus intentionally overlaps (for example JS8Call includes
  JS8, Spotter, and CommStat traffic).
- Opening an unread message updates visible counters immediately from the
  known row transition. The next projection aggregate reconciles the cached
  values with durable state.
- A generation/request fence prevents older aggregate results from replacing a
  newer count state.
- When projection-primary mode is unavailable, the existing bounded in-memory
  calculation remains a compatibility fallback.

## Performance And Concurrency Gates

- Inbox and Reader mode switches perform widget visibility, state, and geometry
  changes only. They perform no database, filesystem, source, or schema work.
- Previous/Next performs no Inbox page/count query and no source refresh.
- The visible document, stable message identity, and `N of M` position are
  committed atomically; navigation input is fenced until that state can paint.
- Focus counter aggregation returns scalar counts only and must measure below
  25 ms p95 on the production-scale database copy.
- Projection invalidations retain the existing at-most-one visible query per
  500 ms coalescer and request/generation fencing.
- Opening a cached/projected text message should acknowledge within 100 ms;
  content loading above 100 ms must remain observable through existing spans and
  must not cause repeated list/model work.
- Resize, theme, font, and splitter events do not query, parse, rebuild counters,
  or recreate reader widgets.
- The reader snapshot is capped at the current 200-row model; detail cache, if
  used, is explicitly bounded.
- Closing Messages or FIO leaves no reader-owned thread, timer, future, or
  QObject lifecycle work.

## Work Packages And Exit Gates

### MIR-0 — Production review and specification

Review both screenshots at original resolution; trace splitter constraints,
responsive behavior, tab/reader flags, projection freeze, focus changes,
counters, and existing performance tests.

Exit gate: the observed states have code-supported causes and a responsive
interaction contract is approved for implementation. **Passed 2026-09-11.**

### MIR-1 — State separation and live counters

1. Separate tab-active lifecycle from reader-open state.
2. Remove reader-driven projection/table freeze.
3. Add one off-thread focus-count aggregate to the existing query result.
4. Apply immediate unread-to-read count transitions locally.
5. Clear reader state before user-initiated scope changes.

Exit gate:

- closing/clearing Reader cannot pause Messages timers or projection queries;
- incoming projection generations update counts without a focus click;
- read transitions decrement every applicable focus count immediately;
- aggregate results obey request/generation fences and the 25 ms p95 budget;
- no extra polling lane or GUI-thread I/O exists.

**Passed 2026-09-11.** The counter result is produced by one grouped scalar
aggregate in the existing coalesced projection worker. It uses only projection
identity/status columns, returns no message bodies or references, and measured
11.409 ms p95 (11.471 ms maximum) over 50 reads of a migrated disposable copy
of the 16,029-row production projection database. Opening/clearing Reader no
longer mutates tab-active lifecycle or freezes the projection table. Request
and generation fences protect aggregate application, and known unread-to-read
transitions update every applicable focus immediately.

### MIR-2 — Content-first reader

1. Add the persistent reader toolbar and mutually exclusive Inbox/Reader modes.
2. Implement Back, Previous, Next, position, stable snapshot identity, boundary
   state, scroll-to-top, and Inbox scroll restoration.
3. Route all existing View/activate paths through the reader without changing
   source-specific content or read semantics.
4. Preserve Open Image and existing detail content.
5. Apply accessible names, tooltips, focus order, and keyboard return behavior.

Exit gate:

- reader viewport targets pass at 1280x720, 1000x700, and 900x560 in Light/Dark
  and Normal/Large Text;
- the list consumes no reader height while Reader is open, and no empty reader
  consumes list height in Inbox mode;
- Previous/Next order and boundaries are deterministic and issue no page query;
- one activation advances exactly one document, and the position never leads
  the displayed content during source-specific rendering;
- Back restores filter/list state and scroll position;
- changing scope cannot retain a prior message body;
- file, JS8, Spotter, CommStat, SitRep, VarAC, Mesh, and projected-message view
  routes retain their existing content/read behavior.

**Passed automated gate 2026-09-11.** The persistent stacked workspace gives
the list and reader mutually exclusive full height. Back, bounded snapshot
Previous/Next, position, top-of-document reset, scroll/selection restoration,
stale-scope clearing, accessibility metadata, Escape, and Alt+Left are covered
by focused tests. Offscreen renders at 1280x720 and 900x560 were reviewed; the
reader body remains the dominant surface. Linux theme/font and live-source
confirmation remain in MIR-3.

### MIR-3 — Integration and Linux production qualification

Run focused Inbox, projection, reader, counter, responsive-layout, theme/text,
shutdown, and performance tests. Review offscreen renders for all required
sizes. In Linux production, confirm live counter updates, message-open/Back,
Previous/Next, focus switching, compact-height reading, and idle CPU.

Exit gate: all automated gates pass and the remaining operator-assisted checks
are recorded explicitly. No next UX slice begins before MIR-1/MIR-2 pass.

Automated evidence on 2026-09-11: 424 message-related tests pass, including the
new reader/state/count suite, projection paging/query fencing, Message
Intelligence, source projection, ingest, responsiveness, and source-specific
content paths. An additional focused 37-test projection/responsive partition
and Python compilation passed; `git diff --check` is clean. MIR-3 remains open
only for Linux production interaction, Light/Dark plus Large Text visual
confirmation, and idle-CPU observation with live sources.

## Model Ownership

The high-reasoning primary model owns UX architecture, lifecycle/concurrency
separation, counter-query design, specification, migration judgment, delegated
diff review, performance evidence, and final integration. Terra owns bounded
reader/layout mechanics. Luna owns focused counter/state and responsive reader
tests. Every delegated diff is reviewed by the primary model.
