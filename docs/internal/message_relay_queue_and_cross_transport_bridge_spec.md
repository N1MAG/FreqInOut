# Message Relay Queue And Cross-Transport Bridge Specification

## Status And Release Boundary

Status: **Deferred / post-2.0 product authority; not authorized for implementation**

This document preserves the product and safety decisions for future held-message
and cross-transport relay work. It does not add a FreqInOut 2.0 feature, change
the 2.0 release gate, enable transmission, migrate operator data, or authorize an
incremental implementation inside an unrelated Spotter, Mesh, BBS, scheduler,
or message-projection change.

Implementation begins only after all of the following are true:

1. the FIO 2.0 multi-radio release is stable;
2. the completed FIO Spotter Activity, Forms, Expect, access-policy, and guarded
   send contracts remain green;
3. the physical Local Mesh receive gates have passed on supported platforms;
4. the protocol-neutral outbound request/result contract described here and in
   `mesh_client_integration_spec.md` has an approved implementation slice; and
5. live RF qualification has an explicit operator test plan.

Where older specifications describe a future `Message Relay Queue`, Store and
Forward, `message waiting`, or Mesh-to-JS8 bridge, this document is the detailed
future authority. Existing specifications continue to control currently
implemented receive, projection, FIO Spotter, BBS, FLAmp Q, and guarded-send
behavior.

## Research Basis

This contract incorporates the review of SuperSpotter 3.0.7 at:

- `/Users/bill/RadioTools/Programs/spotterx3.0.7/js8spotter.py`

The reviewed application demonstrates two useful but distinct concepts:

- a custom JS8 store-and-forward mailbox using `MSG WAITING`, `GET MSG`, and a
  local `sfm_messages` table; and
- a bidirectional MeshCore-channel/JS8-group bridge with queued or immediate
  relay behavior.

The concepts are valuable. Their implementation is not copied. In particular,
FIO must not inherit optimistic delivery state, unbounded retention, ambiguous
list numbers, sender loss, remote third-party storage without policy, raw socket
transmission outside station guards, direction-overloaded relay rows, or a
remote punctuation marker that bypasses local authorization.

## Terms That Must Remain Distinct

### JS8Call Native `MSG`

`TARGET MSG PAYLOAD` is JS8Call's native stored-message transport. FIO already
supports explicit `Send as MSG` through guarded Compose and can ingest a native
stored message into the canonical station message library. Native `MSG` is not
the FIO Message Relay Queue and does not create a station-held FIO mailbox item.

### FIO Message Relay Queue

The Message Relay Queue is a station-scoped durable service containing explicit
delivery intents. A queue item says that an immutable payload is being held for
a destination and records why, under which policy, through which endpoint, and
with what evidence it may advance. It is not a second Inbox or a Spotter-only
message database.

### MeshCore `MESSAGES_WAITING`

`MESSAGES_WAITING` is a MeshCore Companion device signal telling its connected
client to drain messages already queued on the device. FIO's adapter handles it
as transport plumbing through the Companion fetch commands. It is not an
operator-facing `message waiting` advertisement and does not create a FIO held
message unless an independently authorized relay intent exists.

### Publication Services

VarAC BBS, the FIO BBS, and FLAmp Q publish station-library content for access
under their own service and policy contracts. Publication membership is not a
relay-queue lifecycle state. A received or published artifact may be the source
of an explicit relay intent, but it is never silently converted into one.

## Product Mental Model

FIO owns one canonical station message library. Native receipts from JS8Call,
VarAC, FLMsg, FLAmp, MeshCore, Meshtastic, CommStat, BBS, and future adapters
remain source evidence in that library.

The relay system adds separate records:

```text
canonical message or explicit outbound payload
                    |
                    v
            durable relay intent
                    |
        policy + route + endpoint guards
                    |
                    v
      transport-specific dispatch attempt(s)
                    |
                    v
       immutable result and audit evidence
```

Operating groups are metadata and access-policy subjects. They do not partition
the station library into isolated silos and do not own relay queues. Traffic
received for one operating group may be offered or relayed to another context
only when a station policy explicitly permits it. FIO Spotter, FLAmp Q, and the
FIO BBS remain station-scoped services over the canonical library.

## Non-Negotiable Invariants

1. No source message, file, native receipt, or canonical projection is mutated
   to express relay progress.
2. Every relay intent has one stable opaque identifier. Display order and list
   position are never protocol identifiers.
3. Original sender, source family, source endpoint/radio, receive time,
   transport provenance, payload hash, and access context remain inspectable.
4. A remote request may ask for relay consideration; it cannot grant itself
   authorization.
5. Manual review is the default for every new route and both bridge directions.
6. Automatic notification and automatic cross-transport relay default off.
7. A socket/API acceptance is not delivery. States advance only to the level
   proven by retained evidence.
8. One atomic dispatch claim prevents two workers, radios, or retries from
   sending the same intent concurrently.
9. Every automatic attempt passes the same endpoint ownership, selected-target,
   use-radio, schedule, hold, PTT/busy, RF Guard, shared-resource, and shutdown
   protections as an equivalent operator send.
10. Expiry, retry ceilings, recipient quotas, rate limits, loop prevention, and
    durable dedupe are required before unattended behavior can exist.
11. Secrets, Mesh channel keys, signing private keys, and credentials are never
    copied into relay payloads, projections, audits, or diagnostic exports.
12. UI refresh, paint, resize, search, and selection consume bounded immutable
    projections and perform no transport, process, file, schema, or whole-history
    work.

## Canonical Relay Data Contract

The exact SQL schema is an implementation decision, but the durable domain
records must express the following contracts.

### Relay intent

A relay intent contains:

- `relay_id`: stable opaque identifier suitable for a short operator reference;
- `source_message_id`: optional canonical-library identity;
- `payload_snapshot` and `payload_sha256`: immutable content sent by this intent;
- `original_sender` and `created_by`: separate source and operator/service facts;
- `created_utc`, `expires_utc`, and optional operator priority;
- `source_family`, `source_adapter_id`, `source_radio_id`, source channel/group,
  and native source reference when applicable;
- destination kind: callsign, JS8 group, Mesh channel, Mesh node, or a future
  explicitly supported destination class;
- exact destination identity and selected route identity;
- operating-group metadata and the access-policy decision snapshot;
- current lifecycle state and a human-readable reason;
- maximum attempts, completed attempts, next eligible attempt, and cooldown;
- automatic-offer and automatic-dispatch permissions, each explicit and
  separately auditable; and
- immutable idempotency/dedupe identity.

An operator-authored held message may begin without a prior received message,
but it still receives a canonical outbound payload identity. The relay table
must not become an unindexed body store parallel to the message library.

### Route

A relay route contains:

- source family, adapter, radio, channel/group, and direction selectors;
- destination family, adapter/radio, channel/group, and destination kind;
- enabled state and manual/automatic mode;
- sender/callsign/node allow and block policies;
- mapped operating-group/access-policy subjects;
- maximum payload size and supported content types;
- expiry, rate, retry, cooldown, and daily-volume limits;
- maximum relay hops and loop/dedupe window;
- whether notification/offer is allowed separately from payload dispatch; and
- operator-readable purpose and risk explanation.

Routes are station configuration, not message content. A sender-provided `!`,
keyword, group name, or form field never creates or enables a route.

### Attempt and audit evidence

Each attempt contains:

- relay and route identifiers;
- target endpoint generation and radio identity;
- requested, claimed, accepted, started, completed, and observed-ack times when
  each fact exists;
- exact payload hash and transport adapter request id;
- result state, retry classification, and bounded diagnostic detail;
- policy and safety decisions used for that attempt; and
- transmission/delivery evidence available from the adapter.

Audit rows are append-only. Retrying creates a new attempt; it does not rewrite
the prior failure into success.

## Lifecycle And Evidence Semantics

Required semantic states are:

- `held`: durable and eligible for operator review;
- `offered`: an optional waiting notice was confirmed dispatched to the
  configured level of evidence;
- `requested`: the destination requested this stable relay id;
- `dispatch_pending`: authorized and waiting for endpoint/safety availability;
- `dispatching`: owned by one unexpired atomic claim;
- `accepted`: the transport API accepted the request but completion is unknown;
- `sent`: the adapter supplied qualified transmission-complete evidence;
- `acked`: the destination supplied an unambiguous acknowledgement for this id;
- `failed_retryable`: failed with a bounded future retry available;
- `failed_final`: terminal failure requiring operator action or a new intent;
- `expired`: expiry won before a dispatch claim; and
- `cancelled`: explicitly cancelled with retained audit history.

`delivered` may be shown only if a transport provides evidence stronger than
local transmission completion and the specification for that adapter defines
it. FIO must never mark a message sent, delivered, or acknowledged before the
corresponding operation. A failed send cannot remove a message from the held or
retryable set.

Claims use a lease with generation/token ownership. A crashed worker leaves a
recoverable expired claim. Startup reconciliation may return an expired
`dispatching` item to `dispatch_pending` or `failed_retryable`; it may not assume
success.

## Initial JS8 Held-Message Slice

The first implementation slice is deliberately narrow:

- explicit operator-created messages only;
- one base callsign recipient per intent;
- finite required expiry;
- text payload within a qualified bounded size;
- manual offer/send controls;
- query and stable-id pickup through the radio/profile-owned JS8 endpoint;
- automatic `message waiting` notification off by default;
- no remote third-party storage;
- no group-held traffic;
- no automatic cross-transport routing; and
- no implicit conversion of Inbox, BBS, FLAmp Q, Expect, or native JS8 `MSG`
  traffic into held messages.

### JS8 command semantics

The final compact wire tokens require live JS8Call qualification before coding,
but the semantic operations are fixed:

1. query whether the requesting base callsign has held traffic;
2. return a bounded count and stable short references without message bodies;
3. request one stable reference;
4. return the reference, original sender, created/age context, and payload;
5. optionally acknowledge that same stable reference; and
6. return concise errors for unknown, expired, unauthorized, or temporarily
   unavailable references without exposing other recipients' metadata.

List indexes such as `GET MSG 2` are not identities. Bare `ACK` is not accepted
as relay acknowledgement because it can belong to another JS8 exchange. Every
command must be a complete directed-message body addressed to the FIO station,
and caller identity comes from the endpoint's native receive metadata rather
than trusted text inside the payload.

Relay paths require explicit parsing. A relayed requester receives any response
through the qualified JS8 relay construction for that source event; the queue
must not accidentally reply to the intermediate relay station. The exact
origin, relay chain, source endpoint, and response target remain in the audit.

The returned body always identifies the original sender. FIO never relies on
the RF transmitter label for attribution because the transmitter is the FIO
holding station.

### Optional waiting notification

If a later operator explicitly enables waiting notifications:

- notification is scoped to one callsign and one radio endpoint;
- a fresh heard event may make it eligible but never bypasses schedule/safety;
- notification has a per-recipient cooldown, daily ceiling, and finite retry
  count;
- missed/unknown transmission does not become `offered` without evidence;
- acknowledging the notice does not acknowledge or retire the message body;
- notification never exposes sender, subject, or count beyond policy; and
- an operator can pause the service immediately without deleting held traffic.

## Cross-Transport Mesh And JS8 Bridge

Cross-transport relay is a later phase after completion-aware Mesh outbound
support exists. MeshCore and Meshtastic remain separate source families and
adapters even when they share the Relay Queue UI.

### Explicit direction

Every route and attempt names both source and destination. The following are
different operations and cannot share an overloaded `IN`/`OUT` meaning:

- MeshCore channel to JS8 callsign/group;
- JS8 callsign/group to MeshCore channel;
- Meshtastic channel/direct node to JS8;
- JS8 to Meshtastic channel/direct node; and
- locally composed payload sent to one transport with an optional separate
  relay intent for another.

A manual action dispatches to the intent's recorded destination adapter. It
must never infer that every queued row should be sent to JS8 or that every
locally composed row was already sent to Mesh.

### Addressing and identity

Friendly conventions such as `@GROUP body` may assist route selection in an
operator-reviewed compose flow, but the parsed destination becomes a typed
field before persistence. Address syntax is not retained as unvalidated body
text and is not reparsed differently on retry.

Relayed content carries an FIO relay reference and preserves the original
sender/source. `JS8MESH:` may remain a human-readable compatibility label, but
it is not sufficient provenance or loop prevention.

### Authorization

- Both directions have independent route policies and allow/block lists.
- New routes are disabled and manual-review-only.
- A remote `!` or similar marker means “request immediate handling.” It never
  means “authorized to transmit.”
- Automatic mode requires a locally enabled route plus an allowed authenticated
  source identity, accepted source channel policy, allowed destination, and all
  station transmission guards.
- Private/encrypted Mesh traffic is relay-eligible only when its reviewed feed
  policy explicitly allows the destination class. Key possession alone is not
  relay authorization.
- Operating groups may participate in access decisions, but route permission
  is not inferred merely because source and destination share a group label.

### Loop prevention and dedupe

Every relayed envelope contains or is associated with:

- immutable source event/message identity;
- payload hash;
- ordered route trace;
- hop count and configured maximum;
- station relay identity; and
- first-seen and expiry times.

FIO rejects an event when its relay id or source identity is already present in
the trace, the hop ceiling is reached, or a durable source/route idempotency key
already succeeded. A short body/time comparison may suppress transport repeats
but cannot be the primary dedupe key because two senders can legitimately send
the same text.

### Outbound completion boundary

Before any Mesh bridge send is enabled, the Mesh adapter contract must report:

- exact destination kind and identity;
- request id and acceptance time;
- device/API completion or acknowledgement evidence when supported;
- bounded timeout and cancellation;
- retryable versus terminal failure; and
- immutable audit result.

FIO remains truthful when a transport cannot prove delivery. “Submitted to
device,” “transmitted,” and “acknowledged by destination” are separate labels.

## Access, Abuse, And Retention Controls

The service requires:

- station-wide pause without deleting queue contents;
- explicit per-route and per-destination enablement;
- per-sender, per-recipient, and station-total held-message quotas;
- bounded payload size and accepted content types;
- required expiry with a conservative default and operator-visible override;
- retry and notification ceilings;
- source allow/block policies and optional trusted-operator policies;
- rate limiting for query, offer, pickup, and dispatch commands;
- rejection audit without reflecting sensitive policy detail over RF;
- signing/trust display when available without implying unsigned traffic is
  authenticated; and
- bounded retention of terminal attempts followed by policy-driven archival,
  not silent hard deletion.

Remote third-party storage is deferred until it has a separate abuse model,
sender authorization, recipient consent/policy, quota accounting, durable
dedupe token, and live RF qualification. It is not enabled merely for
compatibility with SuperSpotter's `SFM STORE` syntax.

## UI Contract

The initial operator surface is a `Store & Forward` workspace within FIO
Spotter backed by the station Message Relay Queue. It is a view of station
service state, not a Spotter-owned store. Later cross-transport work may expose
the same service from Messages or Local Mesh through focused handoffs.

The bounded list shows:

- stable reference;
- original sender/creator;
- destination and transport;
- lifecycle state stated at the proven evidence level;
- age and expiry;
- next retry/offer eligibility;
- operating-group/access-policy context;
- source and destination endpoint/radio; and
- a concise “why held / why blocked / why failed” explanation.

Primary actions are explicit: `Compose held message`, `Offer`, `Send now`,
`Retry`, `Cancel`, and `View audit`, shown only when valid for the selected
state. Any RF-producing action uses the existing guarded send confirmation and
reports the exact radio, endpoint, target, payload, and blocking reason.

Automatic notification, automatic relay, and route editing live in
administration, not as casual row toggles. Destructive purge is separate from
cancel and requires confirmation. UI refresh is event-driven or operator
requested; no idle timer scans the whole queue or message library.

## Concurrency And Performance Contract

- Queue writes use one serialized service/writer boundary.
- Candidate evaluation is pure against immutable policy, endpoint, and busy
  snapshots; it performs no RF or UI work.
- Dispatch workers claim one intent atomically before external I/O.
- Per-endpoint execution is serialized and composes with the existing radio
  command lane; one stalled destination cannot block unrelated radios.
- Source parsing and body preparation complete before a write transaction.
- Retry scheduling is event/deadline driven and bounded; no high-frequency
  polling is introduced.
- UI list/count queries are bounded and indexed by state, recipient,
  destination, and next-eligible time.
- Startup reconciliation is bounded and does not transmit.
- Shutdown cancels unstarted work, bounds active teardown, and leaves durable
  claims recoverable.

## Migration And Compatibility

No SuperSpotter `sfm_messages` table is imported automatically. A future
explicit importer must preview counts and warnings, operate on a copy or
operator-selected database, and map conservatively:

- `STORED` or `NOTIFIED` becomes `held` with imported/unknown notification
  evidence;
- SuperSpotter `DELIVERED` remains historical “retrieval attempted,” not proven
  FIO delivery;
- unused or absent expiry requires an operator-selected finite expiry before
  activation;
- ambiguous numbering is not preserved as identity;
- duplicate sender/recipient/body rows are reported, not silently merged; and
- imported rows are inert until reviewed under FIO access and endpoint policy.

Existing JS8 native `MSG`, BBS, FLAmp Q, Expect, and Mesh messages remain in
their current stores and projections. Adding the Relay Queue cannot change
their classification, dedupe identity, publication membership, or retention.

## Phased Delivery

### MRQ-0 — Specification only

- Preserve this authority and cross-references.
- No schema, UI, command parser, timer, or transmission changes.
- This is the only phase authorized by the current documentation task.

### MRQ-1 — Core queue and manual administration

- Durable relay intent/route/attempt/audit stores.
- Operator-created callsign-only held messages with expiry.
- Bounded FIO Spotter view and manual cancel/audit.
- No automatic RF and no remote command handling.

### MRQ-2 — Guarded JS8 query and pickup

- Versioned stable-id query/pickup protocol.
- Endpoint-scoped receive parsing and access decisions.
- Guarded manual reply dispatch and exact evidence states.
- Live direct and JS8-relay-path qualification.

### MRQ-3 — Optional bounded waiting notification

- Explicit opt-in per station/route/recipient policy.
- Heard-presence eligibility, cooldown, quotas, and retry ceiling.
- Station pause and visible audit.

### MRQ-4 — Completion-aware Mesh outbound

- MeshCore and Meshtastic typed outbound adapters.
- Channel versus direct-node destination distinction.
- Physical Linux, Windows, and macOS qualification where supported.

### MRQ-5 — Cross-transport routes

- Manual-review bidirectional JS8/Mesh routing first.
- Durable trace, loop prevention, provenance, and source-aware dedupe.
- Automatic routing remains a separate opt-in gate after manual operations
  prove stable.

Remote third-party held-message creation, group-held mailboxes, MQTT/Internet
bridging, APRS gateway behavior, and Reticulum/LXMF propagation require later
independent scopes even if the core queue can eventually support them.

## Acceptance Gates

Every implemented phase requires focused unit, integration, fault, UI, and
shutdown coverage. Before any RF-capable phase ships, acceptance must prove:

1. one relay id cannot dispatch concurrently or twice after success;
2. failure before or during send never becomes sent/delivered;
3. an ambiguous ACK cannot acknowledge a relay item;
4. expiry, cancellation, station pause, use-radio off, hold, RF Guard, active
   PTT, shared-resource conflict, stale endpoint evidence, and shutdown block
   transmission safely;
5. the selected radio/JS8 instance is the one named in the attempt;
6. relayed JS8 requests reply to the true originator through a valid route;
7. source sender/provenance survives pickup and cross-transport relay;
8. identical bodies from different senders remain distinct while transport
   repeats of one source event deduplicate;
9. a remote `!` cannot bypass a disabled/manual route or allow policy;
10. route loops and hop-limit violations are retained as blocked audit events;
11. Mesh queued work dispatches to its recorded Mesh destination and JS8 queued
    work dispatches to its recorded JS8 destination;
12. one disconnected or slow adapter does not freeze UI or unrelated radios;
13. terminal-state retention and importer preview never mutate native sources;
14. compact/wide/Large Text/light/dark UI remains usable without render-path
    I/O; and
15. live operator qualification confirms actual JS8 direct, JS8 relay-path,
    MeshCore, and Meshtastic behavior before those capability rows are called
    supported.

## Release Decision

Message Relay Queue and cross-transport bridging are valuable future station
services. They are not required for FIO 2.0 and must not delay or destabilize
the base multi-radio release. The approved near-term action is documentation
only: retain the architecture, keep current Mesh outbound truthfully disabled,
and begin implementation later as the gated phases above.
