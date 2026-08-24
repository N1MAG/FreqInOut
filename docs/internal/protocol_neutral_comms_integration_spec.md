# Protocol-Neutral Comms Integration Spec

## Intent

FIO should become the operator's common portal for radio and resilient-network
communications without collapsing every protocol into one confusing UI. HF
digital, local voice/manual reports, VHF/UHF/GMRS workflows, MeshCore,
Meshtastic, Reticulum/LXMF, VarAC, CommStat, JS8Call, FastLight, and future MQTT
bridges all have different operational realities. FIO's advantage is to ingest
them cleanly, normalize the useful intelligence, and present the operator with a
coherent answer to the center of gravity:

- where to be
- when to be there
- what to do when there
- who can reach whom
- what important information has moved or still needs to move

This spec covers the full integration direction. It is intentionally
architecture-first so later UI and connector work can proceed without creating
duplicate maps, duplicate message stores, or protocol-specific dead ends.

## Product Goal

Support local mutual-assistance groups and wider emergency communications flows
where not every participant can run HF digital.

The target operating chain is:

- local neighborhood/team
- local NCS or mutual-assistance group
- county
- state
- regional
- national

FIO should help each level use the right available transport:

- voice/manual local reports when that is all an operator can do
- VHF/UHF/GMRS local or county relays
- mesh/Meshtastic/MeshCore local packet paths
- Reticulum/LXMF store-and-forward when available
- HF JS8Call/FastLight/VarAC/CommStat for wider-area traffic
- optional internet-backed services, such as MQTT or CommStat maximum-reach
  propagation, only when explicitly configured

The product experience should feel like FIO understands capabilities and routes
information appropriately, while still leaving the operator in control of
transmit, relay, and SOP decisions.

## Non-Goals

- Do not merge a third-party mesh client UI wholesale into FIO.
- Do not make FIO an offline map-tile management application.
- Do not make internet services required for any core map, message, SOP, or
  local-report workflow.
- Do not hide protocol-specific safety or trust details behind a single generic
  status.
- Do not let map or message UI tabs poll protocol APIs, scan files, or parse
  external databases directly.
- Do not add unattended transmit/relay behavior without explicit policy,
  auditability, and per-protocol safety gates.

## Reviewed Mesh Client Concepts

The local `mesh-client` project at
`/Users/bill/RadioTools/Programs/mesh-client` is useful as a concept reference,
not as code to copy without a separate license and integration review.

Relevant ideas:

- Protocol adapters stay separate for Meshtastic, MeshCore, Reticulum, MQTT, and
  other transports.
- Capability gating keeps UI actions aligned with what a protocol can actually
  do.
- Local DB history is a first-class feature. Messages, nodes, node notes,
  position history, route/path history, delivery state, and identity activity are
  persisted independently from the live connector.
- Reticulum/RMAP discovery stores topology, reachable interfaces, last-heard
  age, hop count, and RF quality fields.
- Path history can score route quality using reliability, latency, freshness,
  and route weight.
- Large route/path render sets are bounded before they reach the renderer; long
  geometries are down-sampled and high-cardinality paths are scoped by current
  view.
- Topology views answer a different question than geographic maps: reachability
  and relay paths, not just location.

FIO should borrow those architecture concepts while preserving FIO's existing
theme, tab model, message intelligence, SOP Builder, RF Guard, and map
projection system.

## Core Principle

Keep transport connectors separate. Unify intelligence after ingestion.

Connector-specific layers own:

- external app/file/API discovery
- polling cadence and backoff
- raw protocol parsing
- source health
- protocol-specific send/write capabilities
- protocol-specific safety and trust checks
- protocol-specific retention and replay semantics

Shared FIO layers own:

- normalized messages/reports
- station/operator/node identity resolution
- operating group and jurisdiction mapping
- topic and condition-alert extraction
- path/link/reachability projections
- map and topology projections
- SOP relevance and action suggestions
- BBS/relay routing previews
- retention, dedupe, and projection policies that span multiple sources

The UI reads shared projections. It does not perform ingestion work.

## Operating Model

FIO should not force operators to think in protocol silos. The user-facing model
is:

- `People and stations`: who is participating and what capabilities they have
- `Groups and jurisdictions`: who the traffic is for and what area it affects
- `Reports`: what information moved
- `Paths`: who can reach whom and by what method
- `Plans and SOPs`: where to be, when to be there, and what to do

Protocols are still visible where they affect safety, trust, delivery, or
available actions. A local GMRS report, a JS8Spotter form, a Reticulum LXMF
message, and a MeshCore room post can all become reports, but FIO must preserve
how they arrived, who relayed them, and whether the path was RF-only,
internet-assisted, store-and-forward, or imported.

The local-to-national flow is a scope model, not a protocol model. A county
report may arrive by local voice, MeshCore, Reticulum, HF digital, MQTT, or a
manual operator entry. Scope drives SOP/BBS/routing decisions; protocol
capabilities drive what FIO can safely do next.

## Protocol Capability Registry

Every source family and connector instance must declare capabilities before the
UI offers actions. Capabilities are data, not hard-coded tab assumptions.

Core capability fields:

- `receive_messages`
- `receive_reports`
- `receive_links`
- `receive_nodes`
- `send_message`
- `send_form`
- `frequency_control`
- `launch_control`
- `bbs_read`
- `bbs_write`
- `store_forward`
- `topology`
- `location`
- `authenticated_identity`
- `rf_only`
- `internet_assisted`
- `read_only`
- `config_write_supported`

Rules:

- The UI enables actions only when the selected source/radio/connector supports
  the relevant capability.
- A source can support ingestion and BBS monitoring without supporting FIO
  frequency control. VarAC standalone is the current example.
- A source can support topology without map coordinates. Reticulum peers and
  some mesh nodes may be reachable but not geographically located.
- Internet-assisted transport is a provenance flag, not a downgrade. It should
  be clear to the operator when traffic was RF-only, internet-assisted, mixed,
  or imported.
- Connector setup and health use the same capability registry, so station-level
  services such as Reticulum or MQTT do not get forced into radio-specific
  settings unless a configured radio truly owns them.

Recommended first capability labels:

| Source Family | Ingest | Send | Frequency Control | Topology | BBS/Store | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| JS8Call | Yes | Yes | Optional via radio route | Yes | No | Multiple instances; source checkpoint per profile/API/files |
| FastLight | Yes | Yes | No | Limited | File-based | FLMsg/FLAmp reports feed topics/BBS/SOP |
| CommStat | Yes | Existing/future | No | Yes | No | Preserve RF-only vs maximum reach/internet-assisted |
| VarAC | Yes | Future | No | Limited | Yes | Monitor/import-only for scheduler/QSY in this release |
| Local/VoiceLog | Yes | Manual | No | Manual | No | Local NCS/operator reports |
| MeshCore/Meshtastic | Future read-only first | Future | No | Yes | MeshCore Rooms | Connector may be RF, MQTT, or both |
| Reticulum/LXMF | Future read-only first | Future | No | Yes | Store-forward | Prefer sidecar/import boundary |
| MQTT | Future read-only first | Future | No | Optional | Optional | Explicitly configured trusted topics only |

## Unified Data Model

### Comms Source

A `CommsSource` identifies where an observation came from.

Fields:

- `source_key`
- `source_family`: JS8Call, FastLight, CommStat, VarAC, Local, VoiceLog,
  MeshCore, Meshtastic, Reticulum, LXMF, MQTT, Import
- `radio_id` when tied to a configured FIO radio
- `app_instance_id` or connector instance id
- `endpoint_or_path`
- `enabled`
- `health`
- `last_seen_utc`
- `capabilities`: receive, send, frequency-control, bbs, store-forward,
  topology, location, authenticated-identity
- `provenance`: RF-only, internet-assisted, mixed, imported, manual, unknown
- `scope_hint`: local, county, state, regional, national, group, direct, unknown

Rules:

- Each source has its own checkpoint identity.
- Multiple JS8Call, VarAC, Reticulum, or mesh instances must never share offsets,
  cached state, or message ids.
- UI filters use `source_family` and friendly labels, not raw paths.
- Source identity must survive migration, backup/restore, and external app
  profile changes. A renamed radio or app path update must not silently create a
  new ingestion stream unless the endpoint/profile identity actually changed.

### Comms Node

A `CommsNode` is an operator, station, device, peer, or relay endpoint.

Fields:

- stable node id
- callsign when available
- mesh/Reticulum/MQTT identity when available
- display name
- known groups and jurisdictions
- role and tier from operator/roster data when available
- capabilities by protocol
- last known grid, lat/lon, state/province, county, and confidence
- trust/auth status by source
- latest activity age

Rules:

- A callsign is not an operating group.
- A node can belong to multiple groups and jurisdictions.
- A mesh-only participant can still be represented and routed into local/county
  workflows.
- A node's capabilities can be inferred from observed traffic, configured
  roster/operator data, or explicit user settings, but inferred capabilities
  should be lower confidence than configured capabilities.

### Comms Report

A `CommsReport` is an actionable or informative piece of traffic.

Fields:

- source key and raw source reference
- from node
- to node, group, or jurisdiction
- topic taxonomy
- severity/status
- report title/summary
- report timestamp and received timestamp
- location/grid/state/county
- auth/trust state
- provenance and delivery scope
- original protocol payload reference
- message viewer target
- SOP relevance hints

Rules:

- Topic filters must apply to all eligible report types: FLMsg/FLAmp,
  JS8Spotter/MCF, CommStat, VarAC, Local Reports, future mesh/Reticulum/MQTT.
- If a user selects topic `Fire`, the map shows stations/reports with fire
  evidence, regardless of whether that evidence came from HF, local, or mesh.
- Raw protocol fields stay available for diagnostics, not primary operator
  display.
- A report may be actionable even if it has no precise location. In that case it
  should still appear in Messages, SOP, BBS/routing previews, and topology, with
  map display falling back to group/jurisdiction/region when possible.

### Comms Link Event

A `CommsLinkEvent` represents observed reachability or signal/path quality.

Fields:

- origin node: the station whose signal quality is being reported
- destination node: the station that heard or was heard
- observer node: the station/source that observed or relayed the report
- direction: origin-to-destination semantics
- protocol/method: JS8Call, VarAC, Reticulum, MeshCore, Meshtastic, manual,
  unknown
- transport provenance: RF-only, internet-assisted, store-forward, imported,
  mixed, unknown
- quality values: SNR, RSSI, hop count, latency, retry count, delivery status,
  confidence
- band/channel/frequency when meaningful
- timestamp and age
- source key

Rules:

- Direction must be explicit. A line on the map should be able to explain:
  "A reported B at +10", "I heard A poorly", or "B heard me poorly."
- The observed-by/source station is distinct from the path endpoints. This is
  critical for third-party reports where my station heard B ask A for signal
  quality, then heard A reply to B.
- Direction arrows must point along the path direction or be omitted when the
  view cannot display them accurately.
- Links may be asymmetric; two stations can have different quality each way.
- Third-party observed links are first-class, not discarded because they do not
  include my station.
- Link events are immutable evidence. Path projections aggregate them, but never
  overwrite the individual observations needed to explain "who said this" and
  "when."
- Do not discard one-way or third-party link events simply because a reciprocal
  event is absent. That asymmetry is operationally useful.

### Comms Path Projection

A `CommsPathProjection` aggregates link events into operator-facing path insight.

Modes:

- `Off`: no path layer
- `My Station`: links involving my configured station identity
- `Selected Station`: links involving the selected station/node
- `Network`: all eligible links for the current filters
- `Relay Candidates`: stations that may bridge from me to a target I cannot
  reach directly

Each projection should include:

- best known path quality
- age
- directionality
- protocol
- source evidence count
- whether the path is direct, reciprocal, asymmetric, or relay-only
- observer summary: my station, selected station, third-party, imported, or
  mixed
- route confidence and why it was selected

### SOP Signal Event

An `SopSignalEvent` is a condition or action trigger extracted from traffic.

Examples:

- configurable MagCon/MAGCON condition message
- local NCS manually logged condition change
- county status escalation
- Reticulum/LXMF message matching a group rule
- MQTT alert from an explicitly configured trusted source

Rules:

- Condition-alert parsing is configured per operating group, not hard-coded.
- Auto SOP invocation is opt-in, audited, and gated by trust and RF Guard when
  radio actions are affected.
- The same signal can feed ControlFreq, Map, Messages, and SOP Builder.

## Map And Topology Model

FIO should keep one primary comms map experience with two complementary views:

Design rule: the map is not a form viewer and not a raw protocol monitor. It is
the operator's spatial and reachability workbench. Every visible control should
answer one of four questions:

- what is happening
- where is it happening
- who can reach whom
- what should I do next

### Geographic View

Answers:

- where are stations/reports
- what is the latest status in an area
- what topics are active
- what groups or jurisdictions are affected

Required behavior:

- Filters compose predictably: view, group, age, topic, path scope, and search
  all narrow the same eligible data set until cleared.
- Layer chips are true toggles. Clicking an active layer disables it.
- `Clear Filters` resets data scope only.
- `Clear Layers` hides optional overlays only.
- The right inspector is the single detail surface; station/report clicks replace
  its content and never open a competing map popup.
- The inspector tabs are `Overview`, `Status`, `Paths`, and `Messages`, with SOP
  actions available when applicable.
- Marker color and icon must be explainable in `Status`: last status, source,
  age, and evidence, not raw question text.
- Topic filters are evidence filters. Selecting `Fire` must show stations and
  reports that have fire evidence in message metadata, observations, local
  reports, or future mesh/Reticulum/MQTT traffic. It must not require the marker
  itself to have been originally created as a fire-specific pin.
- Topic icons follow the same evidence rule. If the active topic is `Fire` and
  the selected/clustered evidence includes fire, the marker uses the fire icon
  even when that evidence also mentions water, weather, comms, or logistics.
  FIO must not repaint unrelated evidence as fire simply because the topic chip
  is selected.
- Search is also an evidence filter. Searching `wildfire` should find the same
  report on the map that Messages can find, provided the other map filters still
  allow it.
- Topic and search refinements must not silently change the selected view chip,
  path scope, or age window. The user-selected map context stays in place until
  the user changes it or clicks `Clear Filters` / `Clear Layers`.
- If a filter combination yields zero traffic, the status line should explain
  which scope is active and whether older or different-source matching evidence
  exists. Zero should feel informative, not broken.
- The age control should be a compact chip/popover with quick ranges and a
  custom range, not a long dropdown. It should read as `Any time`, `Last 3h`,
  `Last 7d`, or a custom label.

### Topology View

Answers:

- who can reach whom
- who heard whom
- who might relay traffic
- what method worked
- where path quality is strong or weak

This can be a map mode first and a graph view later.

Controls:

- path scope: Off, My Station, Selected Station, Network, Relay Candidates
- method layer: JS8Call, FastLight, VarAC, Local, Mesh, Reticulum, MQTT
- direction filter: heard by me, heard me, third-party observed, reciprocal
- age window
- quality threshold
- target station/group/jurisdiction where applicable

Rendering:

- link color indicates quality
- arrow or endpoint decoration indicates direction
- line style indicates method/source when multiple methods are shown
- aggregated links expose evidence count and latest report in the inspector
- high-density link sets must summarize first. Render the best or most recent
  links by default, then let the operator expand to more detail.
- when a station is selected, the inspector should explain inbound, outbound,
  reciprocal, and relay-candidate paths separately.

Topology may be geography-backed or graph-backed. The first implementation can
draw paths on the map, but the data model must be ready for a future graph view
where geographic location is unknown or less important than reachability. This
matters for Reticulum, MeshCore, Meshtastic, MQTT bridges, and store-and-forward
systems where a node may be reachable without a precise map coordinate.

Path controls are layer controls, not hidden filters:

- `Paths` first click enables topology for the current scope.
- `Paths` second click disables topology and restores the previous map context.
- `Show Paths` from the inspector is the same toggle for the selected station.
- `Clear Layers` disables paths, RF planning, propagation, and planning pins
  without changing `Group`, `Since`, `Topic`, search, or current view.
- Direction markers must follow link bearing. If a direction marker cannot be
  rendered accurately at the current zoom or link density, omit it rather than
  showing a misleading sideways arrow.

Path insight should support the operator question from single-rig users:

- "Who can reach this station?"
- "Who can this station reach?"
- "Who can relay between me and that station?"
- "Which report produced this path?"
- "Is this path RF-only, internet-assisted, mesh, store-and-forward, or mixed?"

The map can answer this spatially first. A later graph view can answer it when
nodes lack coordinates or when geography is less useful than topology.

Planning pins are planning/reference records. They are not normal received
traffic. The `Planning Pins` focus shows only saved planning/reference markers,
and the tooltip/labels should avoid wording such as "operator curated" unless
the current user actually created or imported that record.

## Offline Map Strategy

FIO should be useful offline without becoming a large map-tile cache manager.

Recommended model:

- Keep the current local/vector geographic outline as the guaranteed offline
  baseline.
- Include lightweight states/provinces, country boundaries, major region labels,
  and grid overlays where practical.
- Do not require online tiles for any operational workflow.
- If online or cached tiles are later offered, make them optional with a clear
  cache size limit, cache status, and clear-cache action.
- For mesh/Reticulum nodes without coordinates, use topology/list views and grid
  or region fallback rather than pretending to know a precise location.

The map should optimize operational questions, not cartographic detail.

The baseline map should remain a lightweight bundled outline/vector layer for
states, provinces, countries, major regions, and grid context. Online tiles can
be useful later, but they must be optional, bounded, and visibly separate from
the operational projection. FIO should not depend on downloading map tiles to
answer "who can reach whom" or "where is the fire report."

## Mesh/Reticulum/MQTT Integration Strategy

FIO should adopt the architectural concepts from `mesh-client`, not its UI as a
separate embedded application. The first FIO integration should be read-only and
fixture-backed before any live protocol dependency is added.

Recommended approach:

- Add protocol-neutral connector contracts and fixture importers first.
- Use a sidecar/import boundary for Reticulum rather than importing a Reticulum
  runtime into the PySide UI process.
- Treat Meshtastic/MeshCore/MQTT as connector families with their own
  checkpoints, health, identity, and capability declarations.
- Store raw observations and normalized candidates locally before rendering.
- Build topology and report projections from the same local DB used by Messages,
  Map, SOP, and BBS.
- Add send/relay only after read-only ingestion, projection, trust, policy, and
  audit paths are stable.

Mesh/MQTT provenance rules:

- RF and MQTT copies of the same mesh message should dedupe into one operator
  report/message with provenance `mixed` when appropriate.
- MQTT-only traffic must be visibly internet-assisted unless the configured
  source policy says otherwise.
- FIO should not rebroadcast MQTT traffic over RF without a separate relay policy
  and operator confirmation/automation rule.
- MeshCore Rooms and Reticulum/LXMF propagation are store-and-forward
  capabilities. They should feed BBS/relay concepts, but they should not be
  represented as ordinary instant chat unless the protocol actually delivered
  instant peer traffic.

The first prototype should prove that a mesh/Reticulum fixture can add nodes,
reports, link events, and store-and-forward hints to the shared projections
without adding new map code paths.

### Mesh Client Review Findings

The local `mesh-client` project is valuable as a reference for connector
behavior, persistence, and health management. FIO should borrow these concepts,
not embed the application UI.

Patterns to carry into FIO:

- MQTT connectors use explicit connection state, reconnect/backoff, watchdog
  timers, source-specific error reporting, and clear teardown paths.
- Mesh/MQTT ingestion preserves channel/topic identity and distinguishes RF,
  MQTT, and mixed provenance before projecting a user-facing message.
- Reticulum integration is managed through a sidecar boundary with health
  polling, restart controls, response/body caps, and failure isolation.
- Local persistence uses WAL-mode SQLite, schema-version checks, retention
  pruning, and search/index support rather than repeatedly scanning large raw
  files in the UI.
- Message/node/link dedupe is source-aware. A source checkpoint belongs to one
  connector instance and must never be shared with another radio, account,
  topic, room, path, or protocol.
- High-volume topology views are bounded. The service chooses relevant links
  for the current query before the UI renders them.

Patterns not to carry into FIO:

- Do not create a second mesh-only inbox or second unrelated map interface.
- Do not make FIO depend on online map tiles or become a bulk offline tile
  cache manager.
- Do not expose protocol internals as the primary operator UI. Use diagnostics
  for packet, topic, room, sidecar, or protobuf details.
- Do not enable transmit, publish, bridge, or relay behavior merely because a
  connector can receive. Send/write/relay are separate policy-gated
  capabilities.

### Unified Portal Decision

FIO should keep HF, local voice/manual, mesh, Reticulum, and MQTT traffic in one
shared operator model, while preserving protocol context in the UI.

This means:

- Messages is the shared triage funnel.
- Map is the shared geographic and topology workbench.
- SOP Builder is the shared decision/action bridge.
- BBS and relay routing use the same report/topic/group/provenance projection.
- Settings owns protocol-specific connector configuration and policy.

Separate views are allowed when the operator task is genuinely different. For
example, Local Report History can remain distinct from HF message reading, and a
future graph topology view can sit beside the geographic map. However, those
views must consume the same normalized projections so topic, group, station,
source, trust, and age filters behave consistently across the application.

### Offline Map Decision

FIO's required offline map is an operational outline and topology surface, not a
downloaded-tile product.

Baseline:

- bundled lightweight country/state/province/region/grid context
- station, report, status, path, and planning overlays
- optional user/imported reference pins
- topology views that work without precise coordinates

Future optional enhancement:

- bounded regional map packs or cached tiles with an explicit size limit,
  status, and clear-cache action

No operator workflow should require internet map access. If a mesh or Reticulum
node lacks coordinates, FIO should show it in topology, roster, region, group,
or jurisdiction context instead of inventing a map location.

### Protocol-Neutral Event Types

The shared ingestion store should be able to represent at least these event
families:

- `MessageEvent`: raw or normalized message traffic, including JS8 directed
  text, FLMsg/FLAmp files, Spotter MCF, CommStat messages, VarAC messages, local
  notes, mesh messages, LXMF, and MQTT payloads.
- `ReportObservation`: an actionable/informative report with topic, location,
  severity/status, group/jurisdiction, source, and message handoff.
- `LinkObservation`: directional reachability evidence with source, observer,
  subject, target, protocol, method, quality, timestamp, and provenance.
- `NodeObservation`: callsign/node identity, alias, role, group, capability,
  location, and trust evidence.
- `ConditionSignal`: a traffic-derived or manually entered condition-level or
  SOP trigger.
- `RouteCandidate`: a suggested path, BBS placement, relay, or forwarding route
  generated from source capability, trust, SOP, and path evidence.

All of these records are immutable evidence or derived projections. Editing UI
state should not rewrite source observations; it should create a corrected
operator record, alias, annotation, or policy decision that projections can use.

### Cross-Protocol Provenance Labels

Operator-facing provenance must be short and consistent:

- `RF-only`
- `Internet-assisted`
- `Store-forward`
- `Imported`
- `Manual`
- `Mixed`
- `Unknown`

Diagnostics may retain protocol-specific detail such as CommStat maximum reach,
MQTT topic, MeshCore room, Reticulum/LXMF propagation node, JS8Call instance,
VarAC folder, or FLMsg file path. Primary UI should prefer the short label plus
source family, for example `CommStat | Internet-assisted` or
`Reticulum | Store-forward`.

### Connector Health Model

Station Health should summarize every configured source without making the map
or messages UI poll connectors directly.

Each connector health record should include:

- source key and friendly label
- protocol family
- radio id when radio-owned
- station-level connector id when not radio-owned
- last successful ingest
- last error and operator-facing recovery hint
- checkpoint position
- stale/failing/ready state
- capability summary

Connector failure examples:

- JS8Call FIO-B API unavailable
- FIO Spotter MCF folder missing
- VarAC inbox folder unreadable
- Reticulum sidecar stopped
- MQTT broker disconnected

Failures should degrade only the affected connector and any views depending on
it. Scheduler, RF Guard, ControlFreq, and unrelated message/map sources must
continue working.

## Ingestion Architecture

### Connector Contract

Every protocol/app connector must behave like a small service behind a stable
contract. The connector may know about JS8Call APIs, Reticulum identities,
Meshtastic serial packets, MeshCore path payloads, MQTT topics, VarAC DB rows,
or file-system quirks. The rest of FIO should not.

Each connector provides:

- stable connector id
- source family and friendly label
- instance configuration and validation state
- checkpoint read/write for its own source only
- source health and latest error
- capability flags: receive, send, frequency-control, path/topology, location,
  bbs, store-forward, internet-assisted
- `scan_once` or event callback that emits raw source records
- parser that emits normalized candidates for nodes, reports, messages, links,
  and SOP signals
- explicit send/write methods only when the protocol is approved for that
  release

Rules:

- A connector never updates UI widgets directly.
- A connector never mutates another connector's checkpoint or health.
- A connector can be disabled without deleting historical projections.
- A connector can be stale or failed while the rest of FIO remains healthy.
- Connectors are tested with fixtures before they are tested against live
  devices, sockets, sidecars, or external applications.
- Send/write support must be a separate capability, not implied by receive
  support.

This gives FIO room to support multiple JS8Call instances, a VarAC import
folder, a Reticulum sidecar, a MeshCore USB device, and MQTT subscriptions at
the same time without the map or message UI caring how each record arrived.

### Projection Contract

Projection builders transform normalized candidates into UI-ready views. They
are the only layer that decides how raw evidence becomes an operator summary.

Projection builders provide:

- stable fingerprints for cheap UI refresh decisions
- indexed queries for Messages, Map, SOP, BBS, Local Reports, and ControlFreq
- source-family, group, topic, age, trust, status, and text filtering
- explainability fields: why a marker has this color, why a path is shown, why
  a report matched a topic
- bounded result sets for high-cardinality map and path views
- diagnostics for dropped, stale, duplicate, or low-confidence records

Rules:

- UI tabs consume projections, not raw protocol tables.
- Projection queries are composable. A topic filter, group filter, search term,
  source filter, and age filter all narrow the same result set.
- Projections preserve source references so the operator can open the original
  message or diagnostic record when needed.
- Low-confidence identity/location matches are displayed with humility, not
  silently promoted to facts.

### Connector Families

Existing and near-term connectors:

- JS8Call API/files, including multiple active instances
- FastLight / FLMsg / FLAmp files
- CommStat artifacts and reach metadata
- VarAC monitor/import folders and DB
- Local Reports and Local NCS/manual logs
- operator/roster imports

Future connector families:

- MeshCore / Meshtastic via serial, BLE, TCP, or MQTT bridge
- Reticulum sidecar / RMAP / LXMF
- generic MQTT topics for trusted group infrastructure
- manually logged HF/VHF/UHF voice contacts

### Pipeline

1. Connector polls or receives protocol-specific data.
2. Raw immutable event is stored with source identity.
3. Parser creates normalized message/report/link/node candidates.
4. Deduper merges by source-specific stable keys and content fingerprints.
5. Projection builders update message, map, path, roster, BBS, and SOP views.
6. UI receives cheap projection fingerprints and refreshes only affected views.

Requirements:

- Background workers do scanning, API calls, DB reads, and parsing.
- UI refresh reads bounded indexed projections.
- Each connector has backoff, error state, and checkpoint visibility.
- Source checkpoints include path, endpoint, app instance, and radio id.
- Mesh/Reticulum/MQTT checkpoints include connector identity, protocol account
  or node identity, channel/topic/room where relevant, and read position.
- Historical imports can be older than current traffic and still project
  correctly.
- Projection updates are coalesced so large ingest bursts do not repaint the UI
  repeatedly.
- Connector workers emit raw records and normalized candidates; UI tabs never
  call protocol SDKs directly.
- First mesh/Reticulum/MQTT implementation is read-only ingestion unless a
  separate send/write policy is explicitly designed.
- Each connector must be stoppable and restartable independently. A failed
  Reticulum sidecar, MQTT broker, JS8Call API, or VarAC import path must not
  block scheduler, RF Guard, ControlFreq, or unrelated message ingestion.
- Connector health is source-specific. The global Station Health view can
  summarize failures, but the underlying error must name the affected source and
  configured radio/connector.

## Performance And Stability

The integration must preserve FIO's operator trust under load.

Requirements:

- No UI tab directly performs full file-system scans or external DB sweeps.
- Map renders use stable signatures and update only changed layers.
- Message and map filter queries are indexed and paginated where needed.
- Large topology/link sets are bounded by view, age, quality, and node caps.
- Connector failures degrade the affected source only, not the whole UI.
- Station command/scheduler performance remains isolated from message/map
  ingestion.
- Memory caps exist for in-memory graph/link projections.
- Graph/path caches are LRU-bounded by node and query signature. Expensive path
  scoring should be done in services, not in the map widget.
- Startup should restore projections from local DB first, then refresh in the
  background.
- Diagnostics show which source is stale, failed, or still loading.

## Safety, Trust, And Permissions

Trust must be visible but not noisy.

Sources of trust:

- MsgAuth for JS8 text/Spotter messages
- FLAmp/GPG signature state where available
- Reticulum identity trust when implemented
- configured operator roster role/tier/trusted flag
- source provenance and import path
- future MQTT topic/auth configuration

Rules:

- Unsigned traffic is not an alarm by itself.
- If a signature is present, verification state should be displayed.
- Auto SOP invocation requires configured trust policy.
- BBS/relay routing must honor allowed callsigns/groups/jurisdictions.
- Internet-backed propagation is explicitly labeled when it was not RF-only.

## Local-To-National Flow

FIO should allow a report to move from local to broader reach without making a
local-only operator learn HF digital.

Example flow:

1. A local VHF/GMRS operator gives a voice report to local NCS.
2. NCS enters it as a Local Report with topic, location, status, and source.
3. FIO shows it on the map and in Messages/Local Report History.
4. SOP rules identify that it should be summarized to county or state.
5. FIO suggests eligible routes based on capability:
   - local mesh/Reticulum to county gateway
   - HF JS8/FLMsg to regional group
   - VarAC BBS posting
   - CommStat maximum reach if configured and appropriate
6. The operator chooses or confirms the route.
7. FIO records provenance so downstream users know the original source and relay
   path.

Capability-aware routing should be advisory first. Operators decide whether to
transmit, relay, or publish unless a group explicitly enables audited automation.

## Scope, Routing, And Relay Policy

FIO should separate three ideas that are easy to confuse:

- `Audience`: who the message/report is for, such as a callsign, group,
  jurisdiction, BBS audience, or public/shared channel.
- `Scope`: how broadly the information should move, such as local, county,
  state, regional, national, direct, group-only, or private.
- `Transport`: how it can move, such as voice/manual, JS8Call, FastLight,
  CommStat RF-only, CommStat maximum reach, VarAC BBS, MeshCore Room,
  Reticulum/LXMF, or MQTT.

Rules:

- A report can be local in scope even if it later rides HF or Reticulum.
- A report can be national in scope without requiring every participant to have
  HF digital capability.
- Transport selection is a routing decision that should be suggested from
  capabilities, path evidence, permissions, and SOP, not hard-coded to the
  report type.
- FIO should surface when a route changes provenance. Example: `Local voice
  report -> HF FLMsg summary -> CommStat maximum reach` is valuable, but the
  operator should see that the final reach is mixed RF/internet-assisted.
- Relay and BBS placement should be previewed before transmit/write unless a
  group-specific audited automation rule explicitly allows it.

Routing previews should answer:

- Which configured radios/connectors can move this report?
- Which stations or gateways can likely relay it?
- Which group/jurisdiction/BBS audience should receive it?
- Is the route RF-only, internet-assisted, store-and-forward, mesh-only, or
  mixed?
- What trust/auth/signature/provenance is attached?
- What SOP or condition rule is driving the suggestion?

This is the bridge between Map, Messages, BBS, and SOP Builder. It must be a
shared service, not separate ad hoc logic in each tab.

## SOP Builder Integration

SOP Builder becomes the decision bridge between traffic and action.

It should consume:

- topic-tagged reports
- condition-alert signals
- local NCS observations
- path/reachability insight
- BBS/relay routing suggestions
- RF Guard and schedule state

It should produce:

- condition-level changes
- "where/when/what" schedule/action guidance
- suggested relay actions
- BBS file placement rules
- message compose templates
- map filters for affected areas/groups/topics

Automation rules:

- Suggest by default.
- Prompt-to-apply where the group wants semi-automatic workflow.
- Auto-apply only after explicit policy, trust, RF Guard, and audit gates pass.

## UI Integration

### Messages

Messages remains the most detailed read/triage view.

Requirements:

- Source filters include HF, Local, JS8Call, FastLight, Spotter, CommStat,
  VarAC, Mesh, Reticulum, MQTT when implemented.
- Topic filters use the shared taxonomy.
- Opening Messages from Map carries current context: station, group, topic,
  source family, age window, and selected report id where available.
- Message intelligence feeds map, BBS, and SOP projections.

### Map

Map is the operational visualization, not just a pin board.

Primary labels should answer:

- All Stations: who/where is active
- HF Traffic: HF-sourced reports and activity
- Local Traffic: local/NCS/manual reports
- All Traffic: all report-capable sources
- Paths: reachability and relay topology
- RF Planning: propagation and frequency planning overlays
- Planning Pins: operator/RF planning annotations
- SitRep: status-focused reports
- Peer Sched Now: schedule location of peers

Map details should avoid internal words such as `fused`. Use `Multiple Sources`
or `Source: JS8Spotter + CommStat`.

The map inspector is the single selected-object surface. Marker clicks must not
open a second over-map card when the right inspector is visible. The inspector
should use tabs or equivalent compact sections:

- `Overview`: callsign/report identity, area, source, age, summary.
- `Status`: why the marker is this color and what evidence set that status.
- `Paths`: path evidence, direction, quality, method, relay candidates.
- `Messages`: exact message/report handoff context that will open in Messages.
- `Actions`: Send Spotter, filter group/topic, SOP review, and other safe
  context actions when applicable.

Map-to-Messages handoff must preserve the current selected station/report,
group, source family, age window, search text, and topic. If the user is viewing
`Fire` on the map, the Messages view opened from that marker should also be
filtered to the fire evidence unless the user clears it.

#### Recommended Map Interaction Model

The map should feel like a guided visualization tool. The default view should be
calm and obvious; deeper reachability tools should appear as progressive
controls.

Primary controls:

- `View`: All Stations, HF Traffic, Local Traffic, All Traffic, Paths, RF
  Planning, Planning Pins, SitRep, Peer Sched Now
- `Group`: configured groups first, discovered groups only when requested
- `Since`: compact chip with quick ranges and custom range
- `Topic`: shared message intelligence taxonomy
- `Search`: callsign, group, topic, state/grid, report title, keyword

Secondary controls live in `Map Tools`:

- path scope and method layers
- RF planning and propagation controls
- planning pin management
- advanced filters such as state, source, status, trust, and protocol method
- map detail overlays such as callsigns, grids, regions, and population

Behavior:

- Active chips are visually active and can be clicked again to disable when
  they represent optional layers.
- `All Stations`, `HF Traffic`, `Local Traffic`, and `All Traffic` are view
  modes, not optional layers. Selecting one replaces the previous view mode.
- `Paths`, `RF Planning`, `Planning Pins`, and detail overlays are layers and
  can be toggled off.
- If `Map Tools` is open, primary controls must still wrap cleanly on laptop
  screens. The primary controls should not stretch labels and selectors far
  apart or clip chips.
- The map status line should summarize the current projection in plain language,
  for example: `Ready: 12 fire reports from 7 stations, 4 path links shown.`

#### Method Layers

Method layers explain how evidence or reachability moved:

- JS8Call
- FastLight / FLMsg / FLAmp
- VarAC
- CommStat RF-only
- CommStat internet-assisted
- Local voice/manual
- MeshCore / Meshtastic
- Reticulum / LXMF
- MQTT / internet bridge

The first implementation can display method as labels, line styles, and
inspector summaries. Future work can add a dedicated graph topology view without
changing the underlying data model.

#### Station Inspector

The right-side inspector should be the operator's launch point for context
actions.

Actions:

- `Center`: center the selected station/report on the map
- `Show Paths`: toggle selected-station path layer
- `Messages`: open Messages with station/report/topic/group/age context
- `Send Spotter`: open JS8Spotter compose when a JS8-capable source is
  configured
- `SOP`: open or filter SOP Builder for matching group/topic/condition context
- `Filter Group` / `Filter Topic`: quick context filters when appropriate

Tabs:

- `Overview`: identity, area, source summary, last activity, latest important
  report
- `Status`: why the marker is green/yellow/red/blue and which evidence set that
  status
- `Paths`: inbound, outbound, reciprocal, relay candidates, method, quality, and
  who reported each path
- `Messages`: recent relevant reports with topic/source/age summaries

The inspector should never show raw HTML, escaped entities, protocol JSON, or
internal terms such as `fused`.

### ControlFreq

ControlFreq should surface high-value operational context:

- current assigned radio/schedule state
- recent condition alerts
- recent traffic matching active group/topic
- off-schedule and RF Guard warnings
- path/reachability cues if the selected schedule/group has weak reachability

### Settings

Settings owns connector configuration and operating policy.

Future protocol connector setup should be separate from HF radio setup unless a
radio profile actually owns that connector. Mesh/Reticulum/MQTT connectors may
be station-level services rather than per-radio services.

Operating group settings should include:

- supported protocol capabilities
- condition-alert rules
- SOP policies
- FastLight filename policy
- BBS/relay permissions
- jurisdiction/subgroup mapping
- allowed forwarding scopes

## RF Guard Boundaries

RF Guard applies to RF-emitting and frequency-control actions where FIO can
protect equipment:

- radio scheduler assignment
- QSY/manual/timed control
- transmit path conflicts
- shared antenna/front-end conflicts
- unsupported band checks

Mesh/Reticulum/MQTT may later need separate safety concepts such as airtime,
channel, identity, or forwarding guards, but those are not RF Guard unless they
affect radio hardware or transmit paths.

VarAC standalone remains monitor/import-only for frequency control unless a safe
future API changes that contract.

## BBS And Relay Routing

Message intelligence should feed managed BBS and relay decisions.

Examples:

- S2/intelligence reports sweep to configured intelligence folders.
- Fire/infrastructure reports sweep to county and regional folders.
- Local-only reports become summarized HF or Reticulum messages when allowed.
- Operator/group permissions decide who can retrieve what.

Rules:

- Routing previews use the same topic/source/group/jurisdiction projections as
  Messages and Map.
- Long callsign allowlists should be generated from operating group/subgroup and
  roster membership, with manual exceptions.
- Every automatic placement or relay suggestion retains provenance.

## Implementation Phases

### Recommended Build Order

The safest implementation order is:

1. Stabilize existing HF/local projections before adding a new protocol.
2. Add canonical link/path observations for existing JS8Call/CommStat/FastLight
   evidence.
3. Make Map and Messages consume the same topic/group/source/age projection.
4. Add topology/path scopes and right-inspector actions on top of those
   projections.
5. Add fixture-backed mesh/Reticulum/MQTT importers with no live connection and
   no send/write support.
6. Add connector health/status visibility.
7. Add live read-only connector services one family at a time.
8. Add SOP condition-signal rules and optional audited auto-invocation.
9. Add BBS/relay routing previews.
10. Add send/write/relay only after policy, trust, audit, and safety gates are
    complete.

This order protects the current FIO center of gravity. Existing radios,
schedulers, RF Guard, Messages, and Map must become more consistent before FIO
adds live mesh/Reticulum/MQTT complexity.

### Phase 0: Architecture Guardrails

- Add or formalize the source capability registry.
  - Implemented: `freqinout.core.protocol_capabilities` defines transport
    capability hints for current and future sources, and runtime ingest
    descriptors now carry capability, provenance, and scope hints.
- Define connector, raw event, normalized candidate, and projection contracts
  before adding new live protocols.
- Ensure existing map/message/SOP/BBS code paths can consume shared projections
  without directly scanning files or protocol databases.
- Add fixture-driven tests for protocol-neutral reports, nodes, links, scopes,
  provenance, and topic matching.
- Confirm UI actions are capability-gated: no frequency control for VarAC-only,
  no send action for read-only connectors, no relay action without policy.

### Phase 1: Protocol-Neutral Projection For Existing Data

- Normalize existing JS8Call, FastLight, Spotter, CommStat, VarAC, and Local
  Report records into `CommsReport`, `CommsNode`, and `CommsLinkEvent`
  projections.
- Keep existing UI behavior but make filters use the shared projection.
- Add tests for topic, group, age, source, and station filters across source
  families.
- Backfill existing map and message metadata into the projection store without
  changing external app files.
- Preserve the multi-rig source key on every projected record so FIO-A,
  FIO-B, VarAC, CommStat, and future station-level connectors cannot overwrite
  or mask each other.

### Phase 2: Map And Path Stabilization

- Make map filters and layers strictly composable.
- Make Paths a true toggle with My Station, Selected Station, Network, and Relay
  Candidates scopes.
- Fix directional link semantics and quality rendering.
- Ensure right-side inspector is the only detail surface.
- Implement path query tests for:
  - links involving my station
  - selected-station inbound links
  - selected-station outbound links
  - third-party observed links
  - asymmetric link quality
  - relay candidates between two stations
- Make link rendering explainable in the inspector before adding dense visual
  features. The operator should always be able to answer "who reported this
  path and when?"

### Phase 3: SOP Signal Pipeline

- Feed condition alerts and local report events into SOP Builder.
- Add configurable group-specific condition alert patterns.
- Add audited prompt/auto-apply paths.
- Add condition-signal examples as disabled templates, not hard-coded behavior.
- Persist every automatic or prompted SOP invocation as an auditable event with
  source evidence, trust state, and affected radios/plans.

### Phase 4: Mesh/Reticulum Read-Only Prototype

- Add read-only connector fixtures or sidecar import for mesh/Reticulum/MQTT
  topology and message events.
- Project nodes, locations, path links, and messages without send/write support.
- Validate offline map/topology behavior with no tile downloads.
- Start with a fixture-backed adapter and local DB projection tests before
  touching live BLE/serial/MQTT/sidecar connections.
- Preserve source protocol fields for diagnostics while projecting operator
  summaries into the shared Messages, Map, and SOP surfaces.
- The first fixture should include:
  - nodes with and without coordinates
  - direct and third-party link observations
  - store-and-forward messages
  - a topic-tagged local/county report
  - internet-assisted and RF-only provenance
  - a condition signal that is detected but not auto-applied by default

### Phase 5: MQTT And Store-And-Forward Routing

- Add explicitly configured MQTT/Reticulum routing previews.
- Keep internet-enabled flows opt-in and visibly labeled.
- Connect BBS/relay suggestions to operator permissions.
- Label RF-only, internet-assisted, mesh-only, and mixed reach clearly in map
  paths and message provenance.
- Support MeshCore Rooms and Reticulum/LXMF propagation as store-and-forward
  evidence in routing previews before offering write actions.

### Phase 6: Connector Setup UX

- Add guided setup for station-level mesh/Reticulum/MQTT connectors only after
  the projection and read-only paths are stable.
- Reuse Add Radio wizard visual language and core helper pattern.
- Keep station-level connector setup separate from Add Radio unless the
  connector is truly owned by a radio profile.
- Station-level connector setup should ask what the connector represents:
  local mesh, regional mesh, MQTT bridge, Reticulum/LXMF, imported archive, or
  other trusted infrastructure.
- Setup must clearly show whether FIO will read only, monitor live, publish,
  relay, or manage files. The default for new protocols is read-only.

### Phase 7: Send, Publish, Relay, And Automation

This phase is intentionally last.

Before any cross-protocol send/write/relay action is enabled, FIO must have:

- source capability checks
- trust policy
- operating group scope policy
- BBS/relay permission policy
- duplicate/replay protection where practical
- clear provenance labels
- operator preview
- audit log
- RF Guard when radio hardware or transmit paths are involved

Automation levels:

- `Suggest`: FIO recommends an action.
- `Prompt`: FIO prepares the action and waits for confirmation.
- `Auto`: FIO performs the action only when group policy, trust, RF Guard, and
  audit gates pass.

No connector should default to `Auto`.

## Acceptance Criteria

- Selecting topic `Fire` on Map shows every station/report with fire evidence
  across FLMsg/FLAmp, JS8Spotter, CommStat, Local Reports, and future
  mesh/Reticulum/MQTT sources.
- Selecting a station and `Show Paths` displays only path links relevant to that
  station until toggled off.
- Network path view can show third-party observed links such as `A -> B` even
  when my station is only the observer.
- Directional path rendering can distinguish `I hear them`, `they hear me`, and
  `another station reported this path`.
- Opening Messages from Map preserves station, group, topic, source, and age
  filters.
- Local-only reports can participate in SOP suggestions and BBS/relay routing
  without requiring HF digital configuration.
- A manually entered local report can be previewed for county/state forwarding
  using the same routing service that previews HF, VarAC, mesh, Reticulum, or
  MQTT paths.
- A mesh/Reticulum/MQTT fixture can add nodes, messages, reports, and links to
  the same Messages and Map filters without adding a second map or protocol-only
  inbox.
- RF-only, internet-assisted, store-and-forward, imported, and mixed provenance
  display consistently in Messages, Map inspector, BBS routing previews, and SOP
  action prompts.
- Capability gates prevent unsupported actions: read-only connectors do not show
  send/write controls, VarAC-only radios do not show FIO frequency controls, and
  connector failures name the affected source.
- FIO remains useful offline with no map tile downloads.
- UI remains responsive during high-volume ingest and does not flicker or
  rebuild whole map/control surfaces unnecessarily.
- Connector failure degrades only that connector's health and does not block the
  rest of FIO.

## Product Decisions To Confirm

- Which mesh protocol should be first for a read-only prototype: Reticulum/RMAP,
  Meshtastic, or MeshCore?
- Should topology be a map mode first, then a graph view later, or should graph
  topology ship with the first mesh/Reticulum prototype?
- What local-to-county/state default jurisdiction fields should be stored for
  operators and local groups?
- Should MQTT be read-only first, or should it include publish/relay previews in
  the same first slice?
- Which condition alert templates beyond MagNet MAGCON should ship disabled as
  examples?
- Which source should be the first live connector after fixtures: a Reticulum
  sidecar import, a MeshCore/Meshtastic file/API fixture, or MQTT subscription?
- What provenance label should CommStat maximum reach use in the UI:
  `Internet-assisted`, `CommStat Max Reach`, or another operator-facing term?
- Should local/county/state/region/national scope be configured per operating
  group, per local group, or both?
- For future automatic relay, which actions are allowed to become unattended
  after policy approval, and which should always remain operator-confirmed?
