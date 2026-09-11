# Message Reader BBS Action Spec

Status: MRB-0 review/specification, MRB-1 reader action, and MRB-2 bounded
bulk-publication automated gates passed 2026-09-11. MRB-3 Linux production
qualification remains operator-assisted.

Priority: P1 daily-workflow usability. Performance and source-file safety are
release gates.

## Purpose And Authority

The content-first Message reader is the primary place to act on the file being
read. Eligible received FLMsg, FLAmp, and VarAC file artifacts therefore expose
Managed BBS publication at reader level without returning to the list or
opening the BBS administration workspace.

This contract refines `message_inbox_reader_experience_spec.md`,
`message_inbox_controls_spec.md`, and
`varac_managed_bbs_database_manifest_spec.md`. The station Managed BBS catalog,
locations, access, retention, and radio projections remain authoritative. No
new BBS store, legacy copy model, source mutation, or destructive migration is
authorized.

## Existing Capability Review

The Inbox already supports `+BBS` on eligible table rows and opens the shared
Managed BBS location checklist. Accepting the checklist replaces that
artifact's publication memberships atomically; clearing a location removes the
mapping while preserving the received source file. The same catalog is used by
the top-level BBS workspace.

The new reader currently retains Open Image but has no BBS action. Returning to
the list merely to find the row action interrupts the reading workflow. The
existing table delegate also establishes a useful compatibility route, so the
reader addition must reuse its eligibility and persistence behavior rather than
create another publication implementation.

## Reader Interaction Contract

- The reader toolbar shows `+BBS` only for a file-backed FLMsg, FLAmp, or VarAC
  row that can participate in the Managed BBS catalog.
- Ineligible messages show no disabled or explanatory BBS control.
- Clicking `+BBS` lazily loads enabled Managed BBS locations and opens one
  checkbox list. Existing memberships are checked.
- `Apply` is explicit. Checked locations are the complete desired membership
  set for the open artifact; unchecking removes publication from that location.
- Applying an empty set removes the artifact from every Managed BBS location
  without deleting, moving, renaming, or modifying the received source.
- After a successful apply, the reader button shows `BBS · N` when one or more
  locations are selected and returns to `+BBS` when none are selected. A short
  nonmodal confirmation appears beside it.
- The tooltip and accessible name always explain whether the action adds or
  manages BBS locations. The location checklist remains the detailed source of
  access and retention information.
- Previous/Next recomputes eligibility from the new row using cached row data
  only. Back and scope changes clear the reader action state with the reader.

Reader open, Previous/Next, resize, theme, font, focus, and paint paths perform
no BBS database, filesystem, schema, or publication work. An already-warm
five-second publication cache may improve the label, but absence of cached
state must never trigger a lookup. The authoritative membership is loaded only
after the operator invokes the BBS action.

## Inbox Bulk Contract

`More Actions` includes `Publish Selected to BBS...` when at least one selected
row is an eligible file-backed artifact.

- The action operates on at most the existing 200-row bounded Inbox model.
- One location checklist is used for the operation.
- Bulk publication is additive: selected locations are added for every eligible
  selected artifact; existing memberships in other locations are preserved.
  Removal remains an intentional per-artifact reader/table action or a BBS
  Publishing workspace operation.
- Ineligible selected rows and missing source files are skipped and summarized.
- Eligible artifacts are written in one local database transaction. A failure
  rolls back the transaction rather than leaving a partial bulk selection.
- The action does not copy or delete source files and does not initiate live BBS
  reconciliation. Normal bounded publication reconciliation handles projections.

## Performance And Safety Gates

- Eligibility is derived from the current row and its already-projected external
  reference. It does not query BBS configuration.
- The location/membership lookup occurs once per explicitly opened dialog and
  remains bounded by configured locations.
- Reader publication uses the existing atomic mapping API and performs no source
  content read; only file existence/stat metadata is required.
- Bulk work is capped at 200 selected model rows and one transaction.
- No timer, polling lane, source scan, message projection rebuild, or retained
  widget-per-row is added.
- Success never implies that every radio has already materialized its live BBS
  folder. It confirms catalog membership only.
- Missing/changed source files fail calmly and leave prior mappings/catalog data
  recoverable.

## Work Packages And Exit Gates

### MRB-0 — Review and specification

Trace table eligibility, projected-file resolution, location discovery,
membership lookup, atomic mapping, removal, cache invalidation, and reader state.

Exit gate: one shared ownership model and explicit performance boundaries are
documented. **Passed 2026-09-11.**

### MRB-1 — Reader publication management

Add the conditional reader action, lazy shared checklist, exact membership
apply/remove, cached label enhancement, nonmodal confirmation, accessibility,
and navigation/state synchronization.

Exit gate: eligible routes appear; ineligible routes remain absent; checked
state is authoritative when invoked; zero/one/multiple location results render
correctly; reader navigation causes no BBS query; and source files remain
unchanged.

**Passed 2026-09-11.** The persistent reader toolbar owns one conditional
action. Eligibility and navigation are cache-only. Apply reuses the existing
exact-membership operation, reports zero/one/multiple locations, and leaves the
reader open with a nonmodal confirmation. The compact 900-pixel-wide offscreen
render was reviewed with a real `.k2s` detail view and the action remains clear
without reducing document height.

### MRB-2 — Bounded bulk publication

Add the selection-aware More Actions entry and one-transaction additive mapping
operation with skipped/error summary.

Exit gate: mixed eligible/ineligible selection, duplicate artifacts, missing
sources, no locations, cancellation, rollback, and 200-row bounds pass focused
tests without affecting ordinary Inbox rendering.

**Passed 2026-09-11.** `More Actions` exposes the operation only when the
bounded selection contains eligible rows. Duplicate artifacts are collapsed;
one transaction adds the chosen locations while preserving prior memberships.
Missing and ineligible items are summarized and source content remains intact.

### MRB-3 — Integration and Linux production qualification

Run reader, Inbox, BBS catalog/publication, projected-file, responsive-layout,
and performance regressions. In Linux production confirm FLMsg/FLAmp/VarAC
eligibility, prechecked memberships, add/remove, Previous/Next state, compact
layout, and live BBS convergence.

Exit gate: automated tests pass; remaining operator-assisted checks are recorded.

Automated evidence: the complete Messages+BBS selection passed 585 tests with
one platform-dependent skip. Focused reader/BBS, exact-membership, additive
bulk, projected-file, filename-normalization, station catalog, retention, and
responsive-reader paths are included. Python compilation and `git diff --check`
pass. MRB-3 remains open for Linux interaction and live BBS convergence only.

## Model Ownership

Because this extension is tightly coupled to the reader and the existing BBS
mapping methods in one UI module, the high-reasoning primary model owns the
specification, implementation, persistence/safety review, tests, and final
integration. No independent package was delegated for this bounded change.
