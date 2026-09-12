# Compose Messages Workbench Spec

Status: CMW-0 through CMW-5 automated implementation complete 2026-09-11.
Linux production qualification remains an explicit operator gate for real GPG,
NBEMS, Managed BBS, JS8Call send, and selected-target observation.

Delivery governance: all review, implementation, testing, and integration under
this specification must follow `docs/internal/project_delivery_rules.md`.

## Goal

The Messages Compose area should feel like a guided radio workbench, not a
crowded settings form. An operator should be able to choose what kind of
message they are sending, understand which radio will send it, enter the
payload without clipped controls, preview the exact RF/file output, and send or
stage with confidence.

This spec supersedes scattered compose notes in the settings, map, and
SuperSpotter specs. Those specs may still own setup, routing, or map handoff,
but this document owns the Compose user experience and acceptance criteria.

## Design Principles

- Composition is the main task. Do not let radio selectors, file paths, helper
  controls, or previews consume the space needed to write or select message
  content.
- Every compose mode owns its own workbench layout. FLMsg/FLAmp, JS8Call,
  FIOSpotter, and CommStat RF have different operator workflows and should not
  be forced through one cramped grid.
- Embedded Compose may be compact, but must never clip fields. If space is
  constrained, offer the full workbench and keep all controls reachable.
- Compose must comply with the global Large-text accessibility rule in
  `docs/internal/ui_layout_standards.md`; controls should grow from font
  metrics or scroll rather than clipping text for operators using Large text.
- The left compose/setup panel must scroll independently when the embedded view
  cannot show every setup control. It must not clip the bottom half of routing,
  signing, BBS destination, or folder controls.
- The full Compose Workbench is the preferred surface for long forms,
  FIOSpotter forms, and CommStat StatRep with brevity.
- The UI must not hide required fields below an unreachable scroll area.
- In constrained embedded viewports, Compose should visibly promote the full
  workbench button instead of leaving the operator to wonder whether the clipped
  embedded view is broken.
- The summary row must use only the user-defined radio short name, not model or
  capability details. Full radio detail belongs in tooltips or secondary
  preview metadata.
- Any visible compose text that names the selected radio during staging,
  sending, BBS destination selection, guidance, or status must also use only
  the user-defined short name. Full model/capability descriptions are allowed
  only in tooltips, configuration/setup lists, or diagnostic details.
- Group/callsign entry must be autocomplete-assisted from known callsigns and
  configured groups.
- FIO strips leading `@` from group names before JS8Call transmit. The UI may
  accept `@MAGNET`, but payload preview and send command must use `MAGNET`.
- The operator should see exactly what will be transmitted or staged before any
  send/stage action is enabled.
- Heavy data discovery must happen through cached helpers/background work, not
  repeated synchronous UI rebuilds.
- Small fixed-choice controls should be graphical chips, not dropdowns. This
  includes radio source, NBEMS file type, optional VarAC Outbox copy, Managed
  BBS publication, and JS8 send kind. Large or user-defined option sets such as
  forms, folders, categories, BBS locations, and brevity catalogs remain
  dropdowns or checkable lists.

## Compose Modes

### FLMsg / FLAmp

Purpose: create and stage NBEMS/Fast Light files for manual transmission in
FLMsg, FLAmp, or VarAC copy workflows.

Required workflow:

- Select sending radio/profile.
- Select operating group before form selection.
- Filter available custom forms by selected group when mappings exist.
- Preserve `Auto` group when no group mapping is required.
- Show form family and form after group selection.
- Show priority, report title, and timestamp in a compact header.
- Show file-type chips: `FLMsg`, `FLAmp`, or `Both`.
- Show VarAC Outbox copy as an optional radio-specific destination. Managed BBS
  publication is a separate station-service action and must never be presented
  as a VarAC-folder copy.
- Show `Add to BBS` plus one or more enabled logical Managed BBS locations.
  Location labels include concise access/retention context where space allows.
- Show signing option for FLAmp copies without clipping the row.
- Show save folder only when the operator needs to change it. The current save
  destination should remain visible in the summary.
- Form fields must get the majority of the screen.
- Long form fields should be full width. Short fields may be a two-column grid.
- Staging output must be compact and should not steal composing space.

Preferred embedded/wide layout:

- Wide embedded Compose and the full workbench should use two vertical panels:
  a left compose-selection/sidebar panel and a larger right compose-message
  panel.
- Left panel: compact short-name radio chips, `Group`, `Form Family`, `Form`,
  `Priority`, `Report Title`, `Zulu`, `File Type`, optional `Copy to VarAC
  Outbox`, `Sign FLAmp Copy`, `Message Folder`, and Managed BBS controls.
- Right panel: large form field editor/message body with generated filename,
  delimiter/suffix guidance, stage destinations, and exact output preview below
  or beside the editor as space allows.
- The pop-out/full Compose Workbench is the preferred path when the embedded
  tab is too constrained. It must provide the same left/right FLMsg/FLAmp
  structure with more width and height, not merely a scaled copy of cramped
  embedded rows.
- Compact embedded layouts may collapse the panels vertically, but fields must
  remain reachable and unclipped.

Acceptance criteria:

- On a 13-inch laptop window, `File Type`, `Copy to VarAC Outbox`, `Add to BBS`,
  and `Sign FLAmp Copy` are visible and usable.
- `File Type` and fixed destination chips must render as compact inline rows with
  full-height clickable chips; labels or chip text must not be hidden behind the
  next setup row.
- `Compose For` must not span the entire screen in embedded Compose; it should
  leave horizontal room unused rather than stealing vertical composing space.
- Form fields are not vertically clipped.
- In wide embedded Compose and the pop-out workbench, FLMsg/FLAmp setup
  controls appear in a left sidebar and the form editor/message body appears in
  a larger right panel.
- Opening the full Compose Workbench gives the right-side form editor more
  usable room than embedded Compose and preserves the current draft.
- Closing the full Compose Workbench must restore embedded Compose with a clean
  layout pass; no stale splitter positions, overlapping rows, or garbled
  reparented widgets may remain.
- A long message body must have a large editor. If embedded Compose cannot make
  that comfortable, the full workbench/modal is the preferred operator path.
- Selecting Managed BBS publication must not distort the layout. BBS
  destination controls are NBEMS-only unless a separate RF-to-BBS workflow is
  explicitly designed.
- BBS publication updates the one station Managed BBS catalog atomically using
  logical location IDs. Compose must not create/copy files directly in a
  location source folder or a radio's live BBS projection folder.
- When both FLMsg and FLAmp are staged, Managed BBS publishes the FLAmp artifact;
  otherwise it publishes the selected generated artifact. If `Sign FLAmp Copy`
  is enabled, the catalog must reference only the successfully signed and
  locally verified FLAmp artifact. This exact choice appears in preview.
- Failure to sign or verify prevents the FLAmp artifact from being published
  and never falls back silently to an unsigned BBS publication.
- Standard blank form behavior remains `.b2s` driven by form selection, not by
  group default.
- Operating-group delimiter and signed/unsigned suffix policies remain visible
  in preview before staging.
- Existing staging tests continue to pass.

### JS8Call

Purpose: send short JS8Call directed messages or general JS8 traffic through a
selected JS8Call-capable radio.

Required workflow:

- Select sending radio if more than one JS8Call-capable radio exists.
- Show peer schedule / last-heard / path guidance for the target when known.
- Provide a visible `Send To` field with autocomplete.
- When the selected radio's JS8Call instance reports an existing callsign or
  group selection, show a prominent inline cue immediately beside or below
  `Send To`: `Already selected in JS8Call: <target>`. Offer an explicit
  `Use Target` action. The live target is advisory and must never silently
  replace an operator's Compose draft.
- `Use Target` copies the reported live selection into the active Compose
  target using the normal target normalization and validation path. If the
  draft already uses that target, replace the action with a concise
  `Using selected JS8Call target` confirmation.
- A different FIO draft target remains valid. Explain in the cue's tooltip that
  FIO sends its previewed explicit target and clears the conflicting JS8Call UI
  selection through the supported safety path before queueing.
- Provide a visible payload text entry field.
- `Directed Message` requires a target.
- `Traffic` may allow no target.
- Prevent messaging the operator's own callsign.
- Clear selected JS8Call callsign/group targets before transmit if needed so
  the command is sent exactly as previewed.
- Prompt to tune when peer schedule says the target is on another band and FIO
  is configured to tune on band change.

Preferred layout:

- Row 1: compact `Send From`, `Send To`, `JS8 Send` type, with guidance/tune
  action compactly below only when needed.
- Main area: payload editor, large enough for several short RF lines.
- Preview: exact JS8 command.
- Send area: guarded `Send Now` action and status.

Acceptance criteria:

- The text payload field is visible immediately when JS8Call mode is selected.
- `Send From` is constrained to a normal selector width and must not span the
  whole compose frame.
- The preview updates as the operator types.
- Leading `@` is stripped in preview and transmit.
- Self-send is blocked.
- Target autocomplete contains known groups once, without duplicate `@GROUP`
  and `GROUP` suggestions.
- A callsign or configured group selected in JS8Call is discovered without
  blocking Compose, displayed as live JS8Call state, and adopted only after the
  operator activates `Use Target`.
- Selecting another Compose radio invalidates the prior live-target result;
  a late response from the old endpoint cannot appear under the new radio.
- Empty, unreachable, timed-out, or unsupported selected-target responses are
  quiet during composition. They must not clear a draft, disable editing, or
  imply that no destination exists. The guarded send preflight remains the
  authoritative safety check.

### FIOSpotter

Purpose: compose and send FIO-native Spotter MCF forms through JS8Call. The
operator-facing compose label is `FIOSpotter` so it is not confused with an
external JS8Spotter tool or database source.

Required workflow:

- Select sending radio from compact graphical chips. Each chip uses only the
  radio short name defined by the user, such as `FIO-A`; full model/capability
  detail may appear only in tooltips or summaries.
- Select JS8 target with autocomplete.
- Optional MsgAuth signing controls appear only for FIOSpotter and only when
  matching keys exist for the selected target/operator callsign.
- Form category and form selection are prominent.
- The form field editor must be large and scrollable.
- Scrolling must be inside the form editor only when needed; the operator must
  not have to hunt for each next field in a tiny pane.
- Preview shows form name, JS8 payload, MsgAuth state, and send guidance.
- Save-to-Expect remains available for policy review.

Preferred layout:

- Wide embedded Compose and the full workbench should use two vertical panels:
  a left setup/sidebar panel and a larger right form/message panel.
- Left panel: compact `Send From` chips, `JS8 Target`, guidance/tune action when
  needed, `Form Category`, `Spotter Form`, `Save to Expect`, and optional
  `Sign MsgAuth`, key selector, and date-code controls.
- Right panel: large form field editor. FIOSpotter MCF forms prefer a
  one-column layout when labels/options are long or numbered, such as area
  assessment forms, so the editor never needs a horizontal scrollbar.
- Compact embedded layouts may collapse the panels vertically, but setup rows
  and form fields must remain reachable and unclipped.

Acceptance criteria:

- JS8 target, MsgAuth key controls, and form selectors do not overlap or clip.
- `Save to Expect` is grouped below the setup fields, not inline beside long
  selectors.
- `Form Category` and `Spotter Form` must remain visible above the form editor;
  the form editor may scroll, but the setup rows may not be hidden behind it.
- All form fields are reachable.
- The form editor is usable without page-level clipping on laptop-sized
  windows.
- Long form labels and option text wrap or elide within the editor width rather
  than forcing a horizontal scroll area.
- The guidance below `Send From` is a single short operator cue such as
  `Using FIO-B`, `Use FIO-B: last heard on 20M`, or
  `Tune FIO-B: 7.078 MHz 40M`. Longer diagnostic text belongs in the tooltip
  and preview metadata, not in the narrow compose rail.

### CommStat RF

Purpose: compose a short CommStat-format RF StatRep through JS8Call. No
internet CommStat send behavior is included.

Required workflow:

- CommStat RF is presented as CommStat to the operator, but technically sends
  through the selected JS8Call radio.
- Only `StatRep` is a top-level compose type.
- Brevity is an optional addition to StatRep, not a separate message mode.
- When `Add Brevity` is enabled, show the structured brevity builder and also
  allow direct code entry.
- Use current CommStat/SuperSpotter RF wire format.
- Include all StatRep status fields needed to build a complete CommStat RF
  message.
- Keep RF short. Comments should be short and sanitized for RF.
- Preview must show the exact RF payload.

Preferred layout:

- Wide embedded Compose and the full workbench should use two vertical panels
  when there is enough width: a left setup/sidebar panel for send/source
  context and a larger right StatRep builder/preview panel.
- Left panel: compact `Send From`, target guidance/tune action when needed,
  and any high-level send context.
- The CommStat left panel should be narrow; unused sidebar space must be given
  back to the StatRep builder.
- Right panel: `Send To`, `Scope`, `Reported Grid`, `Report ID`, compact
  status-field matrix, `Add Brevity`, direct brevity code entry, and short
  comment.
- Optional brevity builder: two-pair rows for event list/event type,
  status/impact, and public/station response. It must not be a six-control-wide
  strip because that collapses badly on laptop screens.
- Preview/send: exact payload and guarded send status below or beside the
  builder as space allows.
- Compact embedded layouts may collapse the panels vertically, but setup rows,
  brevity fields, preview, and send action must remain reachable and unclipped.

Acceptance criteria:

- CommStat RF mode must not show FLMsg/FLAmp form previews.
- Brevity options are visible when `Add Brevity` is selected.
- Turning `Add Brevity` off must return to normal CommStat StatRep composition,
  not to FLMsg/FLAmp or FIOSpotter form presentation.
- Brevity code generated by the builder is included in the StatRep comment.
- The RF payload preview uses stripped group names and validated brevity codes.
- The send button is disabled until the required fields produce a valid RF
  payload.

## Embedded View Versus Full Workbench

Embedded Compose:

- Good for quick sends and staging simple forms.
- Must never clip controls.
- May be more compact and may use internal scroll areas.
- Should include `Open Full Compose Workbench` for form-heavy tasks.

Full Compose Workbench:

- Non-blocking dialog or dedicated full-height panel.
- Uses the same compose model and widgets where safe, or a shared view-model
  helper if the UI is refactored.
- Provides enough height for the selected mode's natural workflow.
- Closing the workbench restores the embedded view without losing draft state.

Long-term preferred direction:

- Move compose rendering behind mode-specific helper/build methods:
  `build_nbems_compose_view`, `build_js8_compose_view`,
  `build_spotter_compose_view`, and `build_commstat_rf_compose_view`.
- Keep payload generation, validation, and send/stage actions out of widget
  layout code.

## Data And Helper Requirements

Target completion:

- Known callsigns from station metadata, message traffic, map observations, and
  peer schedules.
- Known groups from configured operating groups and message traffic.
- Deduplicate group display so `MR08` and `@MR08` do not both appear.
- Store/display groups without `@` for transmit.

Radio guidance:

- Peer schedule has first priority.
- Last-heard radio/frequency is second priority.
- Path evidence is third priority.
- Selected radio is fallback.
- The inline guidance label must remain one line and must use short radio names
  only. It should never contain the full explanation sentence because Spotter
  and CommStat sidebars are intentionally narrow.
- If guidance recommends a different radio/band and FIO can tune, show a clear
  tune action before send.

CommStat catalogs:

- Read brevity JSON catalogs from configured CommStat path, SuperSpotter path,
  and known local test installs when available.
- Cache parsed catalogs.
- Do not parse catalog JSON on every keystroke.

Forms:

- Discover FLMsg/FLAmp and Spotter forms using the selected radio first.
- Fall back to global settings only when the selected radio has no usable path.
- Rebuild form caches when the compose radio changes.
- Apply operating-group form mappings before presenting the form list.

## Performance Requirements

- Opening Messages Compose should not block on full form/catalog discovery.
- First paint should show mode selector and basic send/stage controls quickly.
- Long discovery should update the UI when ready and show a visible loading
  state if it takes more than a moment.
- Preview updates must be local and fast.
- Catalog/form parsing should be cached by radio/profile path and invalidated
  only when radio, path, or refresh action changes.
- Keystroke-driven preview is pure in-memory work: it performs no filesystem
  discovery or mutation, SQLite query/write, socket call, GPG operation, or
  full widget-tree layout traversal.
- Compose discovery is generation-keyed and asynchronous. Stale results from a
  prior radio, mode, group, or path selection are discarded.
- Expensive stage/sign/verify/BBS-publish and JS8 send/preflight operations run
  outside the GUI thread. The workbench displays progress within one event-loop
  turn, permits only one in-flight primary action, and restores controls on
  success or failure.
- First usable cached paint targets 150 ms; cold paint targets 300 ms before
  background enrichment. A preview edit targets 16 ms at p95 and must remain
  below 50 ms on the bounded acceptance fixture.
- No periodic timer or source scan is added solely for Compose. Background
  discovery is event-driven, bounded, and cached.
- Live JS8Call target discovery is a bounded, one-shot background request on
  entry to a JS8-backed Compose mode, selected-radio change, or explicit
  refresh. It is never issued by keystrokes, preview rendering, resize, paint,
  theme changes, or a polling timer.

## Draft And Action State

- Each compose mode owns a draft containing all operator-entered content and
  meaningful selections. Switching modes preserves the inactive draft.
- Opening or closing the full workbench preserves the same draft and must not
  initiate discovery merely because widgets are reparented or resized.
- `Reset Draft` affects only the active mode, clears its staged-action status,
  and preserves the selected radio unless the operator changes it.
- A successful stage does not erase the draft automatically. The operator may
  explicitly reset after confirming the generated files and publication
  result. This avoids losing content after a partial destination failure.
- Action completion is generation-checked. A result generated from an older
  draft is reported but must not overwrite preview or status for a newer draft.

## NBEMS Transaction Contract

- A stage request snapshots the payload, filename, radio short name, file type,
  signing selection/key, message folder, VarAC Outbox selection, Managed BBS
  selection, and logical BBS location IDs before background work begins.
- Destination filenames are collision-safe and every output is written through
  a temporary sibling followed by a no-overwrite hard-link publication where
  supported, with exclusive-create copying as the portability fallback.
- FLAmp signing occurs before its final destination is made visible. Signed
  output is locally verified before it can be reported as successful or added
  to Managed BBS.
- Managed BBS catalog work occurs in one local transaction after the selected
  publish artifact exists. `upsert_bbs_artifact_path` and
  `set_bbs_artifact_locations` are the ownership boundary. A DB failure leaves
  generated source files intact and reports publication as failed; no live BBS
  directory is modified directly.
- Partial destination success is explicit. The result lists every staged,
  signed/verified, published, skipped, and failed operation and retains the
  draft for correction or retry.
- Save-to-Expect creates a disabled draft without silently replacing an
  existing rule or changing its access/auto-reply policy. A collision opens the
  existing rule for review or requires an explicit replace decision.

## Completion Work Packages (CMW)

### CMW-0 — Contract and architecture review

Primary high-reasoning ownership: reconcile the current implementation with
the station Managed BBS ownership model, define draft/action state, define the
GUI-thread boundary, and preserve existing guarded JS8/MsgAuth behavior.

Exit gate: this specification names one BBS ownership path, one async action
boundary, deterministic artifact selection, non-destructive failure behavior,
and measurable responsiveness targets. **Passed 2026-09-11.**

### CMW-1 — Cheap preview and asynchronous enrichment

Primary high-reasoning ownership for concurrency; focused tests may be
delegated. Split cheap payload/validation preview from radio, form, target,
signing-key, BBS-location, peer/path, and catalog discovery. Cache discovery by
effective inputs, coalesce layout work, and discard stale generations.

Exit gate: typing executes no filesystem/DB/socket/GPG work; cold loading is
visible and non-blocking; stale results cannot overwrite current selections;
preview latency meets the bounded target. **Passed by focused automated tests
2026-09-11.** Target guidance, MsgAuth lookup, form parsing, signing-key
discovery, staging/signing/BBS publication, and guarded JS8 send are worker
owned; preview uses cached destination state only.

### CMW-2 — Responsive embedded and full workbench UX

Bounded UI implementation may be delegated. Establish mode-specific
presentation, compact/wrapped action strips, dominant editors, reachable exact
preview/action, active-mode draft preservation, and stable open/close geometry
at all required viewport/theme/text combinations.

Exit gate: offscreen geometry/render tests and reviewed screenshots pass at
1280 x 720 embedded and 1180 x 780 workbench baselines, plus narrow/Large-text
cases, with no clipping, overlap, stale mode content, or reparenting artifacts.
**Passed by offscreen geometry, workbench round-trip, and mode-draft tests
2026-09-11.** Production visual confirmation remains part of the Linux gate.

### CMW-3 — NBEMS, signing, Managed BBS, Expect, and send actions

Primary high-reasoning ownership. Move action I/O off the GUI thread; make
staging atomic and retry-safe; verify signed FLAmp before publication; update
logical Managed BBS memberships transactionally; protect existing Expect rules;
and retain guarded JS8 target-state preflight and MsgAuth semantics.

Exit gate: the FLMsg/FLAmp/Both × signed/unsigned × Outbox × BBS matrix passes;
missing paths/keys, bad passphrase, signing/verification failure, DB failure,
collision, double-click, close-during-action, and partial success remain calm,
recoverable, and explicit. **Passed by service and UI acceptance tests
2026-09-11.** No destructive migration was used.

### CMW-4 — Integration and qualification

Primary high-reasoning ownership for delegated-diff review, full regression,
spec/work-log reconciliation, and the exit decision.

Exit gate: all automated compose, NBEMS, BBS, Expect, JS8/FIOSpotter/CommStat,
responsive-layout, lifecycle, compile, and diff checks pass. Linux production
qualification covers a real signed FLAmp stage, FLMsg stage, Managed BBS
publication, and JS8 send; any remaining hardware/operator gate is recorded
separately and does not misstate automated completion. **Automated gate passed
2026-09-11:** 71 focused Compose tests and 145 adjacent integration tests pass;
compile and diff checks pass. A full-repository run reached 709 passing tests
before stopping on an unrelated JS8 ingest-policy expectation; this does not
alter the Compose gate and is recorded in the work log.

### CMW-5 — Adopt the target already selected in JS8Call

Primary high-reasoning ownership: define endpoint scoping, stale-result
rejection, unsupported-API behavior, and interaction with the existing guarded
send path. Bounded UI and focused tests may be delegated.

Implementation requirements:

- Query `RX.GET_CALL_SELECTED` asynchronously from the JS8Call endpoint mapped
  to the currently selected Compose radio. Accept `RX.CALL_SELECTED` values for
  both callsigns and `@GROUP` targets.
- Snapshot a monotonically increasing generation plus endpoint identity. Apply
  a result only when its generation and selected radio/endpoint still match.
- Keep the detected live target separate from every per-mode draft. `Use Target`
  is the only action that copies it into the active JS8Call, FIOSpotter, or
  CommStat RF target field.
- Present the live value as observed JS8Call state, not as an FIO recommendation
  or authorization. Use operator-configured radio short names only.
- Preserve the current send contract: the exact FIO preview remains
  authoritative, and FIO clears a conflicting JS8Call selection before
  queueing when the variant supports that action. Adopting the value does not
  bypass preflight or cause an immediate transmission.
- Do not add polling. Refresh only on JS8-backed mode entry, selected-radio
  change, and an explicit lightweight refresh action.

Exit gate: focused tests prove callsign/group/empty/unsupported results,
explicit-only adoption, mode-specific field routing, endpoint/generation stale
rejection, no keystroke/socket coupling, single in-flight behavior, and clean
worker shutdown. Existing Compose and guarded-send acceptance suites remain
green. Linux qualification confirms the cue against a real selected callsign
and group without UI delay. **Automated gate passed 2026-09-11:** 17 focused
CMW-5 tests and 190 Compose/JS8/BBS adjacent integration tests pass; compile,
diff, and offscreen visual checks pass. The 120-edit JS8 payload probe measured
0.750 ms median, 0.899 ms p95, and 1.295 ms maximum.

## Implemented Outcome

- Embedded Compose and the non-modal full workbench share one stable draft but
  use screen-bounded, resize-coalesced geometry. Each mode retains and resets
  only its own draft; the selected radio remains stable.
- FLMsg, FLAmp, and Both are explicit file choices. Optional VarAC Outbox copy
  remains radio-specific. `Add to BBS` selects one or more logical station BBS
  locations and never writes directly to a live BBS projection folder.
- File writes are collision-safe and temporary-first. A signed FLAmp output is
  not exposed or published until local GPG verification succeeds; failure has
  no unsigned fallback.
- The selected BBS artifact is deterministic: FLAmp for `FLAmp` or `Both`, and
  FLMsg otherwise. Catalog membership is committed as one transaction and
  action results use the request snapshot, not whatever the operator selects
  while work is running.
- FIOSpotter Save-to-Expect creates a disabled all-radio draft. It refuses to
  overwrite an existing rule or its access and auto-reply policy.
- JS8Call, FIOSpotter, and CommStat RF use a guarded background send. The UI
  reports API acceptance as `Queued`, never as confirmed transmission, and
  prevents overlapping send attempts.
- JS8-backed Compose modes make the selected radio's live JS8Call target
  visible in a highlighted cue. `Use Target` explicitly adopts the callsign or
  group into only the active mode draft; matching drafts show `Target in Use`.
  The one-shot lookup is endpoint- and generation-scoped, serialized, and
  never runs from preview, typing, layout, paint, or a timer.
- Keystroke preview performs in-memory formatting only. Destination probing,
  peer/path queries, MsgAuth lookup, form parsing, GPG discovery/signing, BBS
  writes, and JS8 socket calls are removed from that path.

## Linux Production Qualification

Use configured production paths and disposable test content. Confirm:

1. Create FLMsg, unsigned FLAmp, and Both; verify the expected sibling files and
   collision-safe names without changing source templates.
2. Create a signed FLAmp file with a real private key; verify its signature in
   FIO/FLAmp. Repeat once with a wrong or unavailable passphrase and confirm no
   unsigned FLAmp or BBS publication is produced.
3. Select two Managed BBS locations, stage, and confirm one station-catalog
   artifact has exactly those memberships and appears through the normal BBS
   projection lifecycle.
4. Copy to a configured VarAC Outbox and confirm the radio-specific destination
   receives the generated file independently of Managed BBS publication.
5. Queue one JS8Call message, one FIOSpotter form, and one CommStat StatRep;
   confirm selected-target preflight behavior and observe the actual transmit
   in JS8Call.
6. Resize embedded Compose and the full workbench at normal and Large text,
   switch among all four modes, and confirm fields, actions, preview, and
   per-mode drafts remain visible and stable.
7. In JS8Call, select a callsign and then a configured group. For each, open a
   JS8-backed Compose mode and confirm the highlighted `Already selected in
   JS8Call` cue names the correct radio and target. Confirm the draft remains
   unchanged until `Use Target`, then shows `Target in Use`. Repeat after
   switching radios and confirm a late prior-endpoint result never appears.

## Non-Goals

- Do not add CommStat internet send.
- Do not make JS8 free-text long-form messaging; RF traffic remains short.
- Do not require External JS8Spotter for FIOSpotter compose.
- Do not redesign the entire Messages Inbox as part of this work.

## Implementation Plan

1. Stabilize current UI so no mode clips required controls.
2. Extract mode-specific compose layout builders inside `MessageViewerTab`.
3. Extract shared target autocomplete and `@` normalization helpers.
4. Move JS8 payload editor into the visible JS8 workbench body.
5. Move FIOSpotter form fields into a large vertical editor with reachable
   scroll behavior.
6. Move CommStat RF into a vertical StatRep builder with visible status fields,
   optional brevity builder, comment, preview, and send action.
7. Make group selection precede FLMsg/FLAmp form selection and filter mapped
   forms early.
8. Cache CommStat brevity catalogs and form discovery by radio/profile.
9. Add targeted tests for visibility, mode isolation, target normalization,
   brevity-as-StatRep, and self-send protection.
10. Run compile checks and narrow compose/send/stage tests after each slice.

## Current Known Defects To Close

- FLMsg/FLAmp setup rows can still feel crowded on narrow laptop windows.
- Send target, VarAC copy, and signature controls must remain visible.
- JS8Call payload entry has appeared missing when the lower panel is off-screen.
- FIOSpotter fields have been clipped and not scrollable enough to complete a
  form comfortably.
- CommStat RF has shown FLMsg form preview content in the past and must remain
  mode-isolated.
- CommStat brevity options must be visible when brevity is enabled.
- Target autocomplete currently risks duplicate group suggestions with and
  without `@`.
- The current implementation has repeatedly failed because mode-specific
  widgets share one setup stack and large minimum heights create blank vertical
  space. The corrective implementation must favor compact setup rows plus a
  large top-aligned mode body, not bigger fixed panels.
- A fresh task is not inherently required if this document remains the source of
  truth and implementation is kept surgical. A fresh task may help only if the
  compose file becomes too risky to reason about in one pass.

## Observed Code Review Notes

Reviewed against `freqinout/gui/message_viewer_tab.py` on 2026-08-27 using the
operator screenshots showing clipped FLMsg/FLAmp rows, an unusable FIOSpotter
form, and CommStat RF losing usable space when brevity is enabled. These are
observations of the current implementation, not new product behavior.

### Layout issues visible in code

- The compose UI is still one shared widget stack built in
  `_build_compose_page`, not mode-specific workbenches. NBEMS, JS8Call,
  FIOSpotter, and CommStat RF all share `compose_setup_box`,
  `compose_field_box`, `compose_splitter`, and `compose_output_box`
  (`message_viewer_tab.py` around lines 4930-5468). This makes each patch
  fight the same vertical budget and explains the repeated loop of fixing one
  mode while breaking another.
- The setup box is forced to a fixed height and capped below the content it is
  asked to contain. `_refresh_compose_layout_geometry` computes visible row
  heights, then clamps them to hard caps: embedded NBEMS 190 px, Spotter 230 px,
  CommStat RF 130 px, JS8Call 130 px; the workbench caps are only 300/250/180
  px (`message_viewer_tab.py` around lines 5535-5576). Any visible rows whose
  combined size exceeds those caps will be clipped instead of pushing content
  into a scrollable or better arranged area.
- The full workbench dialog does not currently create a fuller compose
  experience. It reparents the same embedded widgets into a dialog
  (`message_viewer_tab.py` around lines 5627-5665), so it inherits the same row
  packing, minimum-height, and mode-sharing problems. A modal or non-blocking
  workbench can help only if it gets a purpose-built layout or mode-specific
  panels, not just the embedded layout in a larger window.
- FIOSpotter's target, MsgAuth, key selector, date-code checkbox, key refresh,
  form category, spotter form selector, and `Save to Expect` are split across
  shared rows, with the target/auth row kept as one long horizontal strip
  (`message_viewer_tab.py` around lines 5029-5062 and 5253-5287). This matches
  the screenshot where controls barely fit and the form editor starts too low.
- FLMsg/FLAmp still puts `Group`, `Form Family`, `Form`, `Priority`, then
  `Report Title`/`Zulu`, destination controls, save-under, and optional signing
  into the capped shared setup area (`message_viewer_tab.py` around lines
  5253-5393). The screenshot shows lower destination controls chopped because
  this area has more required rows than the cap permits.
- CommStat RF puts the entire StatRep builder inside `compose_field_box` via a
  stacked RF-fields widget, but that panel is assigned large minimum heights
  while the setup, preview, and send output also remain visible. With brevity
  enabled, `compose_field_box` is given a 520 px minimum and
  `compose_commstat_scroll` a 480 px minimum (`message_viewer_tab.py` around
  lines 8660-8725). On laptop-height windows this crowds out preview/send and
  makes the mode feel unusable.
- CommStat RF's first rows use a six-column grid and the status matrix uses
  four status pairs per row (`message_viewer_tab.py` around lines 5150-5231).
  That is too wide for many laptop widths and contradicts the spec's
  "two-pair rows" direction for brevity and compact status fields.
- The JS8Call plain message kind controls are inserted into the JS8 target row
  rather than the JS8 plain message panel (`message_viewer_tab.py` around lines
  5071-5089). This makes the already crowded target row carry unrelated
  controls and leaves the plain panel with only the text field.
- `QScrollArea.setWidgetResizable(True)` is used for form and RF panels, but
  child widgets carry large minimum heights. When parent containers are also
  hard-capped or minimum-sized, Qt has no clean way to satisfy the constraints;
  it either clips or pushes critical controls off-screen. The screenshots are
  consistent with this constraint conflict.

### Functional and state risks

- Mode isolation is fragile because `_update_compose_preview` owns layout
  visibility, preview generation, send enablement, destination text, style
  updates, completer installation, radio refresh, folder refresh, signing-key
  refresh, and guidance refresh in one method (`message_viewer_tab.py` around
  lines 8480-8890). A layout-only change can accidentally change send behavior
  or stale mode visibility.
- Switching compose modes resets `_compose_active_form_key`, stage paths, and
  radio-target loaded state, then calls `_refresh_compose_forms` and
  `_update_compose_preview` (`message_viewer_tab.py` around lines 6276-6286).
  This may discard draft state more aggressively than the spec's "preserves
  draft state where reasonable" acceptance criterion.
- CommStat RF defaults are filled only when the mode activates and fields are
  blank (`message_viewer_tab.py` around lines 6288-6305). If the selected radio,
  group, or operator profile changes later, the UI may retain stale RF defaults.
- `compose_js8_auth_row_widget` is toggled in preview update, but the reviewed
  layout creates `compose_js8_target_row_widget` and inline auth widgets, not a
  separate auth row (`message_viewer_tab.py` around lines 8690-8750). This is a
  stale-layout smell and should be removed or replaced by explicit Spotter auth
  row ownership.
- The send method is still named `_send_compose_js8_spotter` even though it now
  sends JS8Call, FIOSpotter, and CommStat RF (`message_viewer_tab.py` around
  lines 8892-8975). That naming mismatch is a maintainability risk after many
  edits.

### Performance risks

- `_update_compose_preview` refreshes compose radios, message-folder options,
  target completers, optional BBS targets, signing keys, peer guidance, preview
  HTML, layout geometry, and button styles. It is connected to text changes for
  many fields, including every form field and RF text field. Preview updates
  should be split into cheap payload recomputation versus slower discovery and
  layout recalculation.
- Compose layout geometry must be coalesced onto the next Qt event-loop turn.
  Form/radio/chip changes often emit several signals while the left setup panel
  is still changing visibility; each burst should trigger one splitter/size-hint
  pass, not several immediate passes that make the left panel visibly settle
  over multiple seconds.
- Large-text accessibility guards should be cached per effective tab font and
  invalidated only when new high-use widgets are built or the font changes.
  The guard is important for low-vision operators, but routine compose preview
  updates must not traverse the full Messages widget tree.
- `_refresh_compose_forms` calls discovery and form parsing synchronously when
  modes, groups, radios, families, or forms change. Spotter form discovery and
  FLMsg template parsing should be cached by radio/profile path and should not
  run as part of routine preview updates.
- CommStat brevity catalogs are loaded during widget construction/population.
  Catalog parsing should be cached and refreshed only on path/radio/catalog
  changes, with a visible loading or unavailable state.

## Spec Gaps To Close

- Define the full workbench as a real mode-specific composing surface, not a
  reparented copy of embedded Compose. The workbench should have independent
  geometry rules, larger body space, and optional modal/non-blocking behavior.
- Define minimized embedded Compose as a review/recovery surface. When the main
  Messages tab is too small for comfortable editing, the embedded Compose view
  should visibly promote the full workbench while still allowing the operator to
  confirm the selected radio, draft/status, reset the draft, and recover without
  clipped or garbled controls.
- Add an explicit `Reset Draft` action inside the compose surface and the full
  workbench. Reset clears the current composition and staged output while
  preserving the selected radio/profile unless the operator explicitly changes
  it.
- Add a hard acceptance criterion that setup rows may wrap into two logical
  rows before they clip. Any row containing more than three operator inputs must
  either wrap, move into the main body, or become mode-specific.
- Define minimum usable viewport targets for embedded and full workbench modes,
  for example laptop embedded around 1280 x 720 usable content and full
  workbench around 1180 x 780. Acceptance should require screenshots or Qt
  geometry checks at those sizes.
- Add an acceptance criterion that the send/stage action and exact preview
  remain reachable without scrolling the entire Messages tab.
- Add an acceptance criterion that opening and closing the full Compose
  Workbench preserves the current draft and returns the embedded Compose view to
  a clean minimized-safe layout.
- Add a requirement for stable draft state by mode: switching away and back
  should preserve typed JS8 text, Spotter form values, CommStat fields, and
  NBEMS form values unless the operator explicitly resets or changes the
  underlying form/radio in a way that invalidates the draft.
- Add a requirement that preview update paths must not trigger full layout
  recalculation or file/catalog discovery on every keystroke.

## Recommended UX Direction

- Keep embedded Compose as a compact launcher and quick-send surface. It should
  show the mode selector, radio/source, minimal target/header controls, exact
  preview, and primary action, with no clipped controls.
- Make `Open Full Compose Workbench` the preferred path for NBEMS long forms,
  FIOSpotter forms, and CommStat RF with brevity. The workbench can be
  non-blocking or modal, but it must provide a purpose-built layout per mode.
- For FLMsg/FLAmp, put setup in a compact header and give the form editor the
  dominant space. Destination/signing controls should be reachable as a compact
  action strip or collapsible destination panel, not buried under form setup.
- For FIOSpotter, use three top rows at most: target/send-from guidance,
  form/category/save-to-expect, and optional MsgAuth. Put form fields in a large
  editor below those rows and preview/send in a fixed reachable side or bottom
  panel.
- For CommStat RF, treat brevity as part of the StatRep body. Use compact
  two-pair rows for status and brevity builder fields, keep comment and direct
  brevity code visible, and keep exact RF payload plus `Send Now` reachable.
- Prefer visible labels plus familiar controls over dense six-column grids. The
  goal is a field-ready composing tool that operators choose over external
  FLMsg/FLAmp/FIOSpotter workflows because it is faster and clearer, not just
  because it is integrated.

## Acceptance Test Matrix

- FLMsg/FLAmp selected on a 13-inch laptop-size window: no clipped controls,
  form fields reachable, stage button state correct.
- JS8Call selected: target and payload visible, preview updates, send disabled
  until valid.
- JS8Call target `@MR08`: preview/transmit command uses `MR08`.
- JS8Call target equals operator callsign: send blocked.
- FIOSpotter selected: target/auth/form controls visible, form fields reachable,
  preview shows `F!xxx` payload.
- CommStat RF selected: no FLMsg preview, StatRep fields visible, brevity
  builder visible when enabled, payload validates.
- Switching modes preserves draft state where reasonable and never leaves stale
  widgets visible from another mode.
- Changing compose radio invalidates only radio-scoped form/path caches.
- Selected JS8Call callsign/group state appears without blocking; `Use Target`
  updates only the active draft, and stale endpoint results are ignored.
- Existing tests for NBEMS staging, JS8 guarded send, FIOSpotter native
  integration, CommStat RF compose, and map-to-compose prefill pass.
