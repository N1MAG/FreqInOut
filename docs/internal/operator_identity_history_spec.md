# Operator Identity And Callsign History Spec

## Status

Implemented on the multi-rig private-testing branch on 2026-09-04. Operator
History owns the audited mutation workflow; Ops focus and ingestion consume the
stable identity/effective-dated alias resolver.

## Intent

An operator may occasionally receive a new callsign. FIO must preserve the
association between the new and former callsign without rewriting received
traffic or creating two unrelated people in search, status, schedule, map, and
message intelligence views.

`Change callsign` belongs to **Operators > HF Callsigns > Operator History**.
Ops Center and other views consume the resulting identity history; they do not
own or offer the mutation.

## Identity Contract

- A person/operator has one stable, opaque `operator_id`.
- Callsigns are effective-dated aliases of that identity, not its primary key.
- One alias is current. Former aliases retain their effective interval,
  provenance, and change audit.
- Existing message, observation, check-in, report, and log records remain
  immutable and continue to display the callsign present in the source record.
- Group memberships, role, trust, contact details, notes, pins, schedules, and
  derived history belong to the stable identity where their existing semantics
  allow it. A callsign change must not discard them.
- A portable suffix or formatting normalization is not itself a callsign
  change. Existing canonicalization rules continue to handle those variants.

Suggested Qt-free records:

```text
OperatorIdentity
  operator_id
  current_callsign
  created_at
  updated_at

OperatorCallsignHistory
  alias_id
  operator_id
  callsign
  effective_from
  effective_to
  provenance
  note
  created_at

OperatorIdentityAudit
  audit_id
  operator_id
  action
  old_callsign
  new_callsign
  effective_at
  changed_at
  detail
```

The database must index normalized `callsign`, `operator_id`, and effective
interval boundaries. It must prevent overlapping ownership of the same
normalized callsign.

## Operator History Workflow

The selected operator row exposes `Change callsign…` in its identity/history
actions. The dialog shows:

- current callsign, read-only
- new callsign
- effective date/time, defaulting to now
- optional reason/note
- a concise preservation summary: groups, role/trust, schedule, pins, and
  retained history stay with this operator

Confirmation closes the current alias interval, creates the new current alias,
updates the operator's current display callsign, and appends an audit entry in
one transaction. It is not implemented as delete plus add.

If the new callsign already belongs to an existing identity or overlaps a
historical assignment, stop and show both records. Linking or merging operators
is a separate, explicit review workflow; `Change callsign` must never merge
them automatically.

Operator History presents a compact identity timeline, for example:

```text
K7NEW   Current · since 2026-09-04
K7OLD   Former · 2021-03-12 through 2026-09-04
```

An audited correction/unlink action may reverse a mistaken association without
modifying source evidence.

## Evidence And Attribution Rules

- Search for either the current or a former callsign resolves to the same
  operator identity. The result is labeled with the current callsign and a
  concise `formerly K7OLD` hint.
- Historical rows show the callsign actually transmitted. Operator-focused
  history may group all valid aliases while keeping the matched callsign
  visible.
- Current compose/address actions use the current callsign.
- Resolve ownership against a trustworthy source/event timestamp when present;
  otherwise use FIO receipt time.
- Evidence outside the alias's effective interval is not silently attributed.
  Present it as an ambiguous callsign match available for review.
- Because a callsign can later be reassigned, non-overlapping reuse does not
  prove the same operator. It remains a separate identity unless the user
  explicitly links it.
- Delayed store-and-forward traffic may be received after a callsign change.
  Its trustworthy source/event time may associate it with the former alias;
  receipt time still states when FIO learned about it.

When the changed record is the local station operator, Operator History may
offer a separate explicit action to update the local station profile. It must
not silently rewrite external radio/application configuration.

## Migration And Compatibility

The current `operator_checkins` table is keyed by callsign. Migration must:

1. create one stable identity and one open callsign-history row for each
   normalized callsign owner; exact portable/format variants may remain as
   separate compatibility-roster rows associated with that same identity;
2. add or project `operator_id` without losing the current callsign-keyed
   compatibility API;
3. move identity-owned fields behind Qt-free repository/resolver functions;
4. update consumers incrementally so Map, NCS, SOP, Messages, schedules, and
   imports do not query identity ownership independently; and
5. keep migration idempotent and transactional, with collision diagnostics.

The compatibility roster's `operator_id` index is intentionally non-unique.
Stable identity and alias-ownership constraints are enforced by
`operator_identities` and `operator_callsign_history`; making the roster index
unique would reject valid base/portable rows and block startup migration.

Source ingestion continues storing observed callsign text. It may resolve and
store an `operator_id` reference when interval attribution is unambiguous, but
must not overwrite an existing operator assignment merely because a new
callsign appears in traffic or an imported roster.

## Performance Contract

- Callsign-to-identity resolution is one bounded indexed lookup; it never scans
  messages or rendered Qt rows.
- Autocomplete contains one display record per operator plus its alias keys,
  not duplicate full records or historical messages.
- A callsign change updates the compact identity/autocomplete generation once.
- Operator history and evidence drill-down use descending keyset pagination.
- Backfill of identity references into retained projections is chunked and
  resumable. Search remains usable through alias lookup plus indexed bounded
  evidence queries while backfill is incomplete.

## Acceptance Criteria

- `Change callsign` is available from Operator History management and nowhere
  in the Ops dashboard.
- The change is atomic, audited, and retains operator-owned configuration and
  history.
- Searching either callsign produces one operator focus with current/former
  identity clear.
- Historical evidence retains its original callsign and age.
- New compose actions target the current callsign.
- Conflicts, callsign reuse, and evidence outside effective dates are never
  silently merged.
- Existing callers of the current callsign-keyed operator API continue working
  during migration.
- Identity lookup and autocomplete remain bounded and do not load retained
  message history.
