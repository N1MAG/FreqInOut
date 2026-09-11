# Message Reader File Delete Action Spec

Status: MRD-0 review and MRD-1 automated implementation gate passed
2026-09-11. Linux production qualification remains operator-assisted.

Priority: P1 usability and data-safety refinement for the content-first Message
reader.

## Purpose And Existing Authority

FIO already supports deletion of file-backed FLMSG and FLAMP rows from the
Inbox table. After an explicit confirmation it moves the physical source file
to the operating system Trash/Recycle Bin, removes current FIO cache and
projection state, and records a delete audit. It does not permanently unlink
the source file. The reader did not expose this existing authority.

This specification adds a reader action without broadening source ownership or
delete semantics. It refines `message_inbox_reader_experience_spec.md` and
`message_inbox_controls_spec.md`. It authorizes no schema migration, retention
change, recursive operation, directory deletion, or automatic deletion.

## Reader Contract

- Show `Delete…` only when the open message resolves from already-loaded row
  data to an existing regular file whose canonical origin is `flmsg` or
  `flamp`.
- Do not show the action for JS8Call, Spotter, CommStat, SitRep, Mesh, BBS,
  BBS Archive, unsupported projected rows, missing files, or directories.
- The tooltip says the named file will move to system Trash/Recycle Bin.
- The confirmation identifies the source family and exact path, states that the
  action is normally recoverable through the operating system, and explains
  that FIO will remove the item from current views.
- If cached Managed BBS membership is known, disclose its location count. If it
  is not known, disclose generically that publication stops when the missing
  source is reconciled. Reader presentation performs no BBS query.
- Cancel performs no mutation. Failure leaves the reader open, restores the
  action when the source remains valid, records the failed result, and shows a
  precise warning.
- Success records the delete audit, marks matching projection identity deleted,
  closes the reader, returns to the refreshed Inbox, and gives a concise
  confirmation. It never silently advances to and marks another message read.

## Safety And Performance Contract

- Every operation requires a fresh explicit confirmation; there is no delete
  shortcut or single-click destructive path.
- The target is the exact `FileRecord.path` captured for the open row. Globs,
  directories, unresolved references, and inferred filenames are rejected.
- Deletion uses the OS Trash/Recycle Bin adapter: native Recycle Bin on Windows,
  native Finder Trash on macOS, and desktop trash services on Linux. No direct
  `unlink`, recursive removal, source-folder cleanup, or BBS source copy is
  permitted. If no recoverable system-trash operation succeeds, deletion fails
  closed and leaves the source intact.
- Database cleanup and projection suppression follow the existing audited
  single-file path. A BBS mapping never grants authority to delete another
  source or live projection.
- Visibility and navigation add no filesystem scan, database query, BBS query,
  retained worker, or polling lane. File existence is the only bounded metadata
  check used to decide action visibility.
- The action is disabled while a reader document transition awaits its
  paint-acknowledged commit, preventing deletion of the prior row while the next
  document is being presented.

## Exit Gate

- Existing FLMSG and FLAMP files show `Delete…`; ineligible and missing sources
  do not.
- Confirmation identifies the exact path, recoverable effect, current-view
  removal, and publication consequence.
- Cancel preserves the file and reader. Success moves one exact file, audits the
  result, suppresses its projection, and returns to Inbox.
- Reader Next/Previous synchronization, `+BBS`, responsive layout, source
  deletion, and bulk-delete regressions pass.
- Linux production confirms Trash integration and compact/dark-theme layout.

Automated gate passed 2026-09-11. Focused coverage verifies cache-only action
eligibility, missing-file rejection, exact confirmed target, recoverable-effect
copy, projection/audit handling, source disappearance, reader close, and Inbox
return. Production confirmation remains required before declaring the feature
fully qualified.
