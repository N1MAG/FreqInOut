# Mesh Client Integration Spec

Status: runtime foundation plus initial MeshCore receive bridge implemented  
Scope: local mesh connection configuration, Meshtastic/MeshCore source contracts, passive message/node ingest, future UI routing

## Sources And Documentation

This spec is grounded in:

- Meshtastic Client API: https://meshtastic.org/docs/development/device/client-api/
- Meshtastic Python library: https://meshtastic.org/docs/development/python/library/
- Meshtastic HTTP API: https://meshtastic.org/docs/development/device/http-api/
- Meshtastic MQTT integration: https://meshtastic.org/docs/software/integrations/mqtt/
- MeshCore Companion Protocol: https://docs.meshcore.io/companion_protocol/
- Local reference project: `/Users/bill/RadioTools/Programs/mesh-client`
- Local transport notes: `/Users/bill/RadioTools/Programs/mesh-client/docs/agents/ble-serial.md`
- Existing FIO draft: `/Users/bill/RadioCode/WORK/Reticulum/FIO_Mesh_Adapters_Meshtastic_MeshCore_API_Review_And_Bridge_Spec_2026-07-15.md`

## Product Decision

FIO should treat local mesh as first-class operational traffic, not as a separate app bolted onto the side.

Meshtastic, MeshCore, Mesh MQTT, APRS, and Reticulum/LXMF are distinct source families with distinct provenance. They can be grouped visually as mesh-like/offline sources, but the source family must remain explicit so Inbox, Map, Ops Center, Station Control Center, and future routing do not blur RF-local data with internet-assisted or bridged data.

## Connection Strategy

Bring-up order should be conservative:

1. Meshtastic TCP, when a node exposes WiFi/TCP.
2. Meshtastic USB serial.
3. Meshtastic BLE.
4. Meshtastic HTTP protobuf API.
5. Meshtastic MQTT, disabled by default.
6. MeshCore Companion Protocol and MeshCore BLE/serial after Meshtastic foundation is stable.

USB and BLE are both required field use cases, but neither should be assumed always available. FIO must never import optional hardware libraries or open a device during app startup.

## Configuration Rules

Each mesh adapter config must include:

- `adapter_id`
- `protocol`
- `enabled`
- `connection_type`: `tcp`, `serial`, `ble`, `http`, or `mqtt`
- connection address fields for the selected type
- `send_enabled`, default false
- `store_messages_enabled`, default true
- `map_positions_enabled`, default true
- `bridge_to_reticulum_enabled`, default false

Validation must be specific:

- TCP needs a host and valid port.
- USB serial needs a serial device path.
- BLE needs a saved device id or name.
- MeshCore BLE pairing must be guided as an OS Bluetooth pairing flow; FIO
  should not store the PIN. macOS Bluetooth Settings may not list BLE-only
  MeshCore Companion devices even when the laptop can see their advertisements.
  FIO therefore provides an in-app MeshCore BLE scan that lists nearby
  MeshCore/Nordic UART advertisements and lets the user save the device id/name.
  If pairing is required during connect, the user enters the PIN shown on the
  MeshCore device in the macOS prompt or Bluetooth Settings, then retries.
- HTTP needs a base URL.
- MQTT needs a broker and must be explicitly enabled.

## Runtime Rules

- Mesh adapters must be lazy. No optional mesh dependency can be required for FIO startup.
- Mesh adapters must run outside the Qt UI thread before any live connection work is exposed.
- Passive receive, node discovery, health, map markers, and Inbox integration come before sending.
- Send support remains disabled until the user intentionally enables it for an adapter.
- BLE connection UX must account for serialized scans/connections and platform differences documented by `mesh-client`.
- MeshCore BLE health/errors must distinguish missing device, missing Python
  BLE support, incomplete OS pairing, and non-MeshCore BLE services in plain
  operator language.
- MeshCore BLE scan must run outside the Qt UI thread and must not connect,
  pair, or change saved settings until the user explicitly chooses a discovered
  device.
- USB serial reconnect should be designed to avoid stale open-port failures.

## Connection Lifecycle Rules

First successful MeshCore BLE connection is a bring-up milestone, not the end of
the transport design. Live mesh connections must behave like field links:
present, intermittent, battery-dependent, and sometimes intentionally quiet.

Required lifecycle behavior:

- `Connect` starts the adapter in a worker thread and publishes a health
  snapshot immediately.
- `Disconnect` is explicit and must close BLE/USB/TCP resources, stop polling,
  and mark the adapter disconnected without clearing saved device identity.
- `Reconnect` should be a visible action when a saved connection is enabled but
  disconnected. It should reuse the saved BLE id/name and should not require a
  new scan unless the device cannot be found.
- Auto-reconnect should be conservative: exponential backoff with a visible
  status such as `Retrying in 30s`, capped so a missing device does not burn
  battery or make the UI feel stuck.
- Manual reconnect should bypass the current backoff delay.
- Health must distinguish `disabled`, `ready`, `connecting`, `connected`,
  `retrying`, `needs pairing`, `not found`, and `error`.
- Health rows must also project to the generalized `SourceConnectionSnapshot`
  contract used by UI surfaces. The shared lifecycle vocabulary is
  `connected`, `reconnecting`, `away`, `disabled`, and `config_error`; detailed
  protocol states may remain in MeshCore-specific diagnostics.
- A disconnected adapter's last nodes/messages may remain visible, but must be
  labeled with age/staleness so the user does not mistake stale mesh data for
  current local traffic.
- Health-only lifecycle changes update source chips and status labels only.
  They must not clear retained mesh messages, nodes, routes, topic hits, map
  markers, or force a map/WebEngine redraw.
- Mesh management keeps saved/connected source identity separate from BLE scan
  results. Always-visible health chips and connection indicators use the
  configured or advertised short device name when available and do not expose
  raw BLE UUIDs unless no human-readable identity exists. Nearby devices
  discovered by Scan MeshCore belong in the Found selector, not in the connected
  source indicator.
- Mesh channel review tables use semantic row states (`accepted`, `pending`,
  `ignored`) and render those states with theme-aware foreground and background
  colors. Light-mode green/yellow row fills are not valid in dark mode unless
  paired with readable dark-theme text contrast.
- Changing saved connection settings stops the current adapter before starting
  the replacement adapter.
- App shutdown must always stop mesh workers before Qt exits.

## Channel And Addressing Rules

Mesh traffic must preserve protocol-specific channels while projecting into the
shared FIO view contracts.

Mesh channels are source-scoped operating contexts. They are similar to
operating groups because they shape who hears what, but they are not the same
thing. Operating groups describe human/mission membership; mesh channels
describe protocol feed boundaries. A user or device may belong to multiple mesh
channels and multiple literal groups such as a local radio club, community mesh,
neighborhood group, or response team.

Channel fields:

- `channel_id`: stable protocol identifier when known, such as Meshtastic
  channel index or MeshCore room/channel id.
- `channel_name`: user-facing label when known.
- `channel_role`: public, private, direct, admin, telemetry, or unknown.
- `channel_privacy`: plain-language hint such as `public`, `encrypted`,
  `direct`, or `unknown`; never display secrets or PSKs.
- `mapped_groups`: zero or more FIO operating groups that should receive the
  channel's traffic context.
- `retention_window`: per-channel retention policy such as `24h`, `7d`, `30d`,
  or `keep pinned`.
- `inbox_enabled`, `ops_enabled`, `map_enabled`, and `topic_scan_enabled`.
- `default_category`: how traffic should be interpreted before message-level
  topic parsing. Values are `auto`, `social`, or `ignore`. This allows Public
  to remain useful for map/topic scanning while keeping casual chatter out of
  Ops Center when desired.

Configuration paths:

- `From Scratch`: the user creates channel records manually with name, protocol
  id/index, purpose, mapped groups, visibility, topic scanning, map use,
  retention, and alert policy.
- `Pull From Device`: FIO reads configured channels from the connected device,
  shows a review screen, and lets the user choose which feeds become FIO
  channels before anything is saved.

Device import review rules:

- Imported channels must be staged for review, not silently saved.
- Device-discovered channels are raw device facts until accepted. Staging must
  preserve a user's existing accepted/ignored decision and must not reset
  surface toggles when the device is seen again.
- The review should show channel name/id, inferred role, privacy hint, expected
  traffic use, retention, and default visibility.
- The user can accept, ignore, rename for FIO display, map to one or more
  operating groups, and tune Inbox/Ops/Map/topic-scan behavior plus the default
  category.
- Channels ignored during review should remain discoverable later without
  creating Inbox, Map, or Ops Center noise.
- FIO must never display or persist channel secrets/PSKs.
- Manually added private channels are a `from scratch` declaration that the
  feed exists. They do not mean FIO has joined the channel. The operator must
  join the channel on the mesh device with the channel encryption key, then mark
  the feed as joined/key-ready in FIO before accepting it into Inbox, Ops
  Center, Map, or topic scanning.
- Device-discovered private channels may be treated as key-ready only when the
  connected device reports the channel as already configured. FIO may keep a
  non-secret hint such as `device key`, but must not copy or expose the raw key.

Projection rules:

- Mesh packets are stored raw first. Promotion into the shared observation layer
  requires an accepted channel policy that allows at least one FIO surface.
- If a channel policy is pending or ignored, the raw packet may remain in the
  mesh store for diagnostics/review, but it must not appear in Inbox, Ops
  Center, Map, or topic scanning.
- If `topic_scan_enabled` is off but another surface is allowed, the message may
  appear in that surface without contributing topics.
- Messages with no explicit channel may use an accepted default public channel
  policy for that adapter/protocol. Directed packets may use the accepted direct
  policy when the protocol exposes a recipient but omits a clean direct-channel
  id.
- Retention is enforced per channel policy during runtime maintenance. Expired
  raw packets and their observation projections are removed together; `keep
  pinned` must preserve the feed until pin support can distinguish pinned rows.
- Inbox grouping uses `source_family + adapter_id + channel_name/channel_id`.
- Map filtering may use channel/group when it helps answer "where is this
  traffic relevant?"
- Direct messages should project as person/operator traffic, not group/channel
  traffic, when the protocol makes that distinction.
- Unknown channel names should render as short neutral labels such as `Channel
  0` rather than raw opaque ids.
- Public/default channels remain available as local intelligence feeds. If
  enabled, public-channel topic hits should appear in Inbox with clear source
  categorization and also feed Ops Center. They should not become high-attention
  alerts without severity evidence.
- Private channels should normally be Inbox-enabled when accepted, because they
  represent an intentionally configured feed.
- Telemetry/admin channels should default to Inbox off and health/map context
  only unless the user opts in.
- Direct messages should remain visible unless explicitly muted.
- Channel selection for sending remains disabled until receive, identity, and
  routing semantics are validated on real hardware.

Source reference contract:

- `mesh:{transport}:{adapter_id}:{message_id}` is the only mesh source-ref
  shape that represents operator message traffic. These rows may enter Inbox,
  Ops Center, Map, and topic scanning when channel policy allows those surfaces.
- `mesh-node:{transport}:{adapter_id}:{node_id}` represents node/contact/router
  topology, not a message. These rows must never appear in Message Inbox even
  when the source family is `meshcore` or `meshtastic`; they belong in Map mesh
  node views, source health, topology/operator detail, and Ops context.
- `mesh-channel:{transport}:{adapter_id}:{channel_id}` represents configuration
  or policy state. These rows must never appear as Inbox message traffic.
- Rebuild jobs must preserve this split. Rebuilding observations from raw mesh
  data may refresh node and channel projections, but it must not clear accepted
  `mesh:` message projections or replace them with topology rows.

Default channel policies:

| Channel type | Inbox | Ops Center | Topic Scan | Map | Retention |
| --- | --- | --- | --- | --- | --- |
| Public/default | On, categorized as public mesh | On, severity-gated | On | If mappable | 24h |
| Private/named | On | On | On | If mappable | 7d |
| Direct | On | On for recent/direct context | On | Sender location only when labeled | 30d |
| Telemetry/admin | Off | Health/context only | Off by default | Node/location only | 24h |
| Ignored | Off | Off | Off | Off | none |

## Tags, Topics, And Severity

Mesh can bring in a lot of informal local traffic. FIO should not make users
read everything to know what matters.

Tagging rules:

- Raw protocol tags, hashtags, channel names, and known group names are
  preserved as source metadata.
- FIO topics are normalized separately and should use the operational taxonomy
  already used by Inbox, Map, Ops Center, and SOP suggestions.
- Mesh message text is topic-scanned at ingest when the channel policy allows
  topic scanning. Explicit protocol/source topics are preserved, and inferred
  FIO topics are added without duplicating labels.
- If a mesh message has text but no operational topic hit, it is categorized as
  `Social`. `Social` is searchable and filterable in Inbox, but it must not
  create operator attention in Ops Center by itself.
- Channel policy may override automatic topic interpretation with
  `default_category=social` or `default_category=ignore`. This is useful when a
  Public feed should remain available for Inbox/Map review but should not turn
  casual local chatter into Ops Center noise.
- Lightweight topic corrections may suppress known false positives without
  changing the raw message. Example: MeshCore routing chatter about a flood
  advertisement must not be promoted as a Weather/Flood incident unless other
  operational evidence supports it.
- Auto-tags should be explainable: fire, medical, comms, water, power, road,
  weather, logistics, direct, position, telemetry, test, and admin are good
  initial categories.
- Severity defaults to `info`; it can rise only from explicit emergency markers,
  known distress keywords, trusted structured forms, local priority policy, or
  operator manual pin/escalation.
- A topic/tag must not cause a map concern by itself. It needs visible evidence:
  location, repeated local corroboration, trusted source, or structured status.
- Users must be able to pin, clear, or correct topic/severity interpretation
  without editing the raw received message.
- Row actions for corrections should start simple: `Change Topic`,
  `Mark Social`, `Mute Topic For Channel`, and `Ignore Similar`. These actions
  write projection policy/correction records, then rebuild observations from raw
  mesh messages.

## Message Projection Rules

Mesh messages must enter the same durable traffic pipeline as HF and JS8
traffic, with source provenance intact.

Required message fields:

- `source_family`: `meshtastic`, `meshcore`, `mesh_mqtt`, `aprs`, or
  `reticulum_lxmf`
- `adapter_id`
- `message_id`
- `from_node`, `to_node`, `channel`
- `received_time`
- `text`, `subject`, `summary`
- `topics`, `severity`
- `hop_count`, `route_type`, `direct_receive`, `via_node`, `path_hops`, `snr`,
  `rssi`, `packet_id` when available
- channel-policy metadata: accepted surface list, channel id/name/role/privacy,
  mapped groups, key readiness state, and policy source
- raw payload stored for diagnostics, not primary UI display

MeshCore node/contact ingest:

- MeshCore BLE Companion support must treat device contacts as canonical
  `MeshNode` records, not as UI-only diagnostics. Contact sync data that includes
  advertised name, public key/prefix, last advert time, hop/path length, RSSI/SNR,
  and advertised lat/lon must be normalized into the shared node contract.
- MeshCore contact/node ingest must be best-effort. Unsupported firmware,
  timeout, or missing contact-sync frames should produce a health warning or
  empty node list without blocking channel discovery, message ingest, Inbox,
  Ops Center, or Map rendering.
- The shared worker/manager is responsible for polling `list_nodes()` and
  publishing node events. UI views must consume stored mesh-node projections and
  health summaries instead of calling MeshCore BLE methods directly.
- Future MeshCore, Meshtastic, Mesh MQTT, APRS, and Reticulum/LXMF adapters must
  expose location, routing, and peer identity through the same source contracts
  whenever the underlying protocol provides those facts.
- MeshCore Companion contact frames and `NewAdvert` push frames must decode the
  official firmware layout exactly: response/push code at byte 0, public key
  bytes 1-32, contact type byte 33, flags byte 34, out-path length byte 35,
  out-path bytes 36-99, advertised name bytes 100-131, last-advert timestamp
  bytes 132-135, advertised latitude bytes 136-139, advertised longitude bytes
  140-143, and last-modified timestamp bytes 144-147. A decoder that reads
  coordinates from the path area will silently make all MeshCore routers look
  unmappable.
- MeshCore live push frames that carry contact/advert data must be promoted to
  `MeshAdapterEvent(event_type="node")` so the shared worker can persist them
  without waiting for a manual contact sync.
- Node persistence must preserve the last known non-empty name, route facts,
  grid, and GPS coordinates when later frames omit those fields or advertise
  zero coordinates. Sparse refresh frames may update freshness and signal, but
  must not erase useful map data.
- MeshCore location precedence is: explicit packet/contact GPS, device/self
  position, decoded RF advert position, retained previous node position,
  route-derived first known repeater/router, then unknown. Every downstream view
  must label sender, declared, and route-derived locations distinctly.

MeshCore route-direct rules:

- `direct_receive=true` and `hop_count=0` mean the frame was heard directly with
  no repeater or mesh relay. This is route context only; it must not reclassify a
  Public or private channel message as a Direct/DM feed.
- A message with a real channel id, such as MeshCore `channelIdx=0` or
  `channelIdx=2`, must match the accepted channel policy for that id before any
  Direct/DM fallback is considered. A `to_node` value like `channel` is a channel
  broadcast marker, not a Direct message marker.
- MeshCore companion channel messages may deliver the sender as a text prefix
  such as `N1MAG MOBL2: Test`. When no explicit sender field is present, FIO
  should conservatively split that prefix into `from_node=N1MAG MOBL2` and
  `text=Test`, while preserving raw source metadata for diagnostics.
- Inbox, Ops Center, and Map detail views should label route-direct messages as
  `Direct receive` or `No repeater` so users understand the planning value
  without confusing routing with privacy.

Inbox rules:

- Mesh traffic appears in Inbox with source-specific source labels and an
  operator-facing `Mesh` focus chip that groups MeshCore and Meshtastic while
  preserving protocol provenance.
- The Mesh focus chip loads only `mesh:` traffic observations. Mesh node,
  contact, router, telemetry, and channel-policy projections remain hidden from
  Inbox unless a future UI explicitly introduces a topology inbox.
- The `Any time` age filter is unbounded. It must show all rows allowed by the
  active source/group/search filters and must not be treated as zero seconds
  old, no rows, or a rebuild trigger.
- Changing the age filter must not cause accepted mesh messages to vanish when
  the next mesh refresh arrives. Mesh refresh and Inbox filter state are
  independent concerns.
- The default view should prioritize recent direct messages, local actionable
  traffic, and pinned/flagged messages.
- Inbox filters must support channel selection by adapter, channel role, channel
  name/id, mapped group, direct/public/private, and topic.
- Public-channel topic hits appear automatically when the public channel is
  accepted for topic scanning, but they must be visibly categorized so the user
  understands they came from broad local intel rather than a private group.
- Operators can open Compose from a mesh message only when that source has a
  validated send path; otherwise Compose should offer HF/JS8 reply options and
  explain that mesh send is not enabled.
- Duplicate packet ids must upsert instead of creating repeated inbox rows.

Ops Center rules:

- Recent mesh traffic feeds the global attention queue using the same severity,
  recency, source trust, topic, and geography gates as existing traffic.
- Direct messages should influence the network-building/social-fabric view even
  when they are not emergency traffic.
- Mesh health issues should appear as station-connection health, not as radio
  failures.
- Mesh ingest readiness must be summarized as an explicit operator state:
  `needs_channels`, `needs_key`, `needs_accept`, `decoder_needed`, or `ready`.
  This prevents a connected-but-empty mesh card from looking broken. The summary
  should tell the user the next useful action in one short sentence.

## Mapping Rules

Mesh mapping must be useful without turning the map into a wall of points.

Node map rules:

- The Map tab must expose an explicit `Mesh Nodes` view selector so operators
  can intentionally inspect local mesh topology apart from message traffic.
- Mesh Nodes must use the shared map render contract: stable node ids,
  coalesced payload updates, bounded marker/cluster rendering, one-shot
  auto-fit, and no routine map-shell reloads. This is required before MeshCore
  is treated as a model for Meshtastic, APRS, Mesh MQTT, or Reticulum/LXMF map
  layers.
- Selecting `Mesh Nodes` auto-fits once to the visible mappable mesh-node set.
  Live node updates after that must preserve the operator's current viewport
  unless the operator explicitly chooses to fit/refresh the view again. If no
  node has GPS, grid, or a route-derived location, the view should show a clear
  empty state instead of putting node records in Message Inbox.
- A node with lat/lon may render as a mesh node marker when map positions are
  enabled.
- A node with only grid may render at grid precision with lower confidence.
- A node without location must stay in Inbox/Ops/Operator context and must not
  create a fake map point.
- Markers must show source family, adapter id, callsign/short name, last heard,
  hop count, and battery/signal when available.
- Mesh Nodes mode must provide geographic reference without making the operator
  hunt for context. When Mesh Nodes is selected, city/town labels should be
  enabled for that view even if general city labels are off, with a lower
  population threshold appropriate for local mesh coverage. This is a view
  rendering rule, not a persistent global map-preference change.
- City/town reference labels are part of the Mesh Nodes view payload and must not
  cause the map shell to reload when toggled by view entry.
- Mesh node clusters must preserve enough node names in detail for operators to
  recognize important routers. Do not summarize dense mesh clusters with the
  short limits used for ordinary traffic report clusters.
- Mesh map detail must be mesh-oriented: show node identity, count, source,
  freshness, routing facts, signal/location confidence, and a clear explanation
  when placement is route-derived rather than GPS/grid. It must not fall back to
  generic `Operational Report` wording for mesh-node selections.
- Mesh map actions must be role-aware. Router, repeater, relay, telemetry, and
  cluster selections should offer `Center`, `Show Routes`, and copy/inspect
  actions only. `Message Node` is shown only when FIO has a single targetable,
  user-like node identity.
- Stale nodes should fade or move to a historical layer based on age.

Message map rules:

- A message maps only when it has explicit location, grid, a mappable structured
  report, or a source/operator location that is clearly labeled as sender
  location.
- Mesh message location confidence must be displayed and sorted by evidence:
  GPS/lat-lon from the message or node first, grid next, then route-derived
  approximate location from the first known repeater/router, and finally unknown.
  Route-derived locations are acceptable directional context when direct sender
  location is unavailable, but must be labeled as approximate.
- If a message lacks its own location but the sending node/contact has a known
  location, FIO may project the message using that sender-node location with
  `location_confidence=sender_lookup` and provenance type `sender_node`. This is
  more specific than route-derived relay placement but still must not imply the
  incident itself occurred at the sender unless the message content supports it.
- Direct messages from a node with a known location should not automatically
  imply an incident at that node unless the message content says so.
- For routed mesh messages, the first repeater/router in the known path may be
  used as a general-area proxy for the sender when no GPS/grid exists. The map
  marker and station detail must explain that the location is route-derived, not
  the sender's exact position.
- Route-derived placement must be stored on the shared observation as
  `location_confidence=route_derived` with a `provenance.location_source` object
  naming the relay/router source. Map detail must render this as
  `Route-derived approx`, optionally including the relay/router label.
- Topic map filters should carry to Inbox and back using the same
  source-aware handoff contract used by existing map/message actions.
- High-volume future sources such as APRS and Mesh MQTT must cluster, filter by
  recency, and avoid full map rebuilds on every packet.
- Mesh node and traffic updates must be pushed into the existing map page when
  the map is already initialized. Routine packet/node refreshes must not reload
  the WebEngine page, steal focus, resize/minimize the main window, or repeatedly
  auto-fit the viewport.
- Top control-bar mesh source indicators must collapse stale scan/configuration
  rows that refer to the same physical node. A connected node must not appear as
  both a healthy and warning chip because an older BLE scan row says it was not
  found.

Routing-aware station detail:

- MeshCore and Meshtastic routing facts are optional detail enrichment, not a
  primary dashboard surface.
- Store routing context when available: route type, direct/flood/repeated
  receive, hop count, via node, path hints/hashes, RSSI, SNR, and freshness.
- Map station detail may show compact route facts such as `Heard: Direct`,
  `Heard: 2 hops`, `Path: Flood`, `Via: repeater`, and `Signal: RSSI/SNR`.
- Inbox and Ops Center should show routing only when it changes operator
  judgment, such as stale multi-hop traffic, local direct traffic, or degraded
  receive confidence.
- Missing routing data must never block message, Inbox, Ops Center, or Map
  rendering. Unknown values should simply be omitted.
- FIO should not become a full MeshCore route analyzer unless operator workflow
  proves that route visualization is central. Dedicated mesh tools can remain
  the place for deep route debugging.

## View-Contract Requirements

Meshtastic and MeshCore are now first-class source families in the
view-contract layer.

Required views:

- Ops Center attention queue
- Message Inbox
- Compose workbench
- Map context
- Operator directory
- Station Control Center

Meshtastic-specific fields:

- `node_id`
- `channel`
- `portnum`
- `hop_count`
- `snr`
- `rssi`
- `packet_id`

MeshCore-specific fields:

- `node_id` or public-key/hash identity when exposed by the Companion Protocol
- `channel_id`
- `channel_name`
- `message_kind`
- `ack_state`
- `route_type`, direct/flood receive hints, hop/path hints, and returned-path
  metadata when exposed safely by the Companion Protocol
- BLE endpoint id/name

Common mesh actions:

- `Inbox`: opens source-aware Inbox filters.
- `Map`: opens only when mappable context exists.
- `Compose`: enabled only when the selected adapter has send enabled and the
  protocol send path is implemented; otherwise route to available HF/JS8 reply
  options.
- `Reconnect`/`Disconnect`: available in Settings and Station Control Center.
- `Pin`: keeps important nodes/messages visible in Ops Center and Map without
  altering raw data.

Common traffic fields remain the protocol-neutral FIO traffic fields.

## Settings Ownership

The current Settings `Add Radio` workflow is for HF radios and SDR-like radio
profiles that own radio software, rig control, launch settings, RF Guard, and
frequency-plan assignment.

MeshCore and Meshtastic devices should not be squeezed into that exact wizard
as if they were HF radio profiles. They should enter Settings through a broader
station-connection workflow:

- `Add Connection`
- choose `Radio`, `SDR`, `Meshtastic`, `MeshCore`, `Reticulum`, or future source
- show only the fields for that connection type
- route saved connections into Station Control Center and Ops Center using the
  same view-contract model

This keeps the operator mental model clean: radios are controllable station
assets; mesh devices are local traffic/network connections that may or may not
be attached to a specific radio operating plan.

## Initial Implementation Slice

Implemented now:

- `freqinout.core.mesh.settings`
- `freqinout.core.mesh.models`
- `freqinout.core.mesh.adapter_base`
- `freqinout.core.mesh.meshtastic_adapter`
- `freqinout.core.mesh.manager`
- `freqinout.core.mesh.store`
- Meshtastic source-view contract
- Settings `Local Mesh` panel for station-level mesh connection configuration
- Mesh protocol selector for Meshtastic vs MeshCore provenance
- TCP, USB serial, BLE, HTTP, and MQTT configuration fields
- explicit receive/map/send policy controls, with send disabled by default
- lazy USB serial-port discovery that does not require PySerial at startup
- validation-driven setup guidance in Settings
- non-Qt mesh connection manager for adapter lifecycle, health snapshots, and event publication
- Qt-safe mesh connection worker wrapper for future threaded passive receive
- Meshtastic pub-sub receive subscription with queued event draining
- durable passive mesh message store
- mesh health persistence for device/status UI
- mesh message projection into the shared observation pipeline for future Ops Center, Inbox, and Map use
- mesh node persistence for known/last-heard local mesh nodes
- mesh node projection into the shared observation pipeline with location, grid, hop, and callsign/name context
- manager-to-store event sink so live workers can persist health/messages without UI coupling
- app runtime sidecar that starts the mesh worker only when Local Mesh is explicitly enabled
- settings-saved restart hook so Local Mesh connection changes take effect without restarting FIO
- main-window shutdown hook so the mesh worker stops before app exit
- slow node polling cadence so node/map readiness does not make UI refresh feel heavy
- shared source-family labels for Mesh, MeshCore, and Meshtastic
- MeshCore BLE safe-connect adapter using the Companion/Nordic UART service
  UUIDs from `mesh-client`, with pairing-aware error guidance
- in-app MeshCore BLE scan/select workflow in Settings so BLE-only advertised
  devices can be selected even when macOS Bluetooth Settings does not list them
- mesh channel policy model with public/private/direct/telemetry defaults,
  review-state gates, per-channel retention, and Inbox/Ops/Map/topic-scan
  controls
- private/encrypted channel key-state gating: private feeds require an explicit
  joined/key-ready state before they can be accepted into Inbox, Ops Center,
  Map, or topic scanning. Device-discovered private channels may be treated as
  joined because the key is already configured on the device; manually added
  private feeds remain pending until the user confirms the channel key is
  configured. Raw encryption keys must not be stored in plain settings; any
  future FIO-side join workflow must use a secure secret store or device-native
  channel write path.
- durable `mesh_channel_policies` store for accepted, ignored, and pending
  device-discovered channels, including private channel key readiness metadata
- protocol-neutral channel polling boundary for future device channel import
- MeshCore companion normalization boundary for `getChannels()` and
  `getWaitingMessages()` shapes used by `mesh-client`, including channel
  privacy/key readiness, channel/direct message projection, stable message IDs,
  and companion `pathLen` to hop/direct-receive context
- MeshCore adapter consumption of companion-style channel/message APIs when a
  supported client exposes them, while keeping raw BLE connection validation
  separate from protocol decoding
- MeshCore Companion Protocol raw BLE receive bridge for the Nordic UART
  Companion service: FIO can start notifications, issue conservative
  `DeviceQuery`, `GetChannel`, and `SyncNextMessage` commands, decode
  channel-info/contact/channel-message frames, stop channel discovery on
  Companion error/no-response, and project hop/direct-receive hints from
  `pathLen`. This is receive-oriented; it does not enable mesh sending.
- persistent MeshCore BLE event-loop ownership so the live BLE client,
  notifications, channel discovery, waiting-message polling, and disconnect all
  run on the same long-lived asyncio loop owned by the adapter
- mesh ingest readiness assessment that distinguishes no reviewed feeds,
  private channels needing a joined/key-ready state, no accepted feeds, accepted
  policy with MeshCore decoder still pending, and fully ready feeds
- Settings `Mesh Channels` review surface with staged Public/Direct defaults,
  manually staged private feeds, accepted/ignored review states, explicit
  private-channel joined/key-ready status, and persisted policy gates scoped by
  adapter/protocol
- device-discovered channel staging from the runtime worker into Settings review
  without overwriting reviewed choices
- Settings Local Mesh must show an obvious connection indicator sourced from the
  active runtime mesh health row: Connected, Needs attention, Not connected, or
  Disabled. Channel discovery alone is not enough feedback.
- Connected mesh devices must also surface as compact source chips in the
  Station Control Center top bar only when they are saved/configured devices.
  The chip is a source health indicator and routine connection menu, not an HF
  radio control card.
- Top control/source chips must use the best operator-facing configured device
  name, such as the saved device name or MeshCore node name. Raw BLE UUIDs,
  scan ids, transient adapter ids, and unsaved discoveries belong in Local Mesh
  settings/details only and must never appear as daily control chips.
- Mesh control chips are grouped by protocol family so MeshCore, Meshtastic,
  and future local-network sources remain understandable when more than one is
  configured. A single generic `Mesh` chip is not sufficient once multiple mesh
  protocols exist. Each protocol chip lists saved-device `Connect` actions,
  `Disconnect`, channel management, and `Add Device...`.
- The top control/source rail follows a `SourceControlItem` display contract:
  configured radios render as direct focus chips, while each configured local
  mesh protocol renders as a compact dropdown chip. The dropdown may list only
  saved/configured mesh devices for routine `Connect` actions. Newly discovered
  or unsaved BLE devices must not appear in the daily source rail; they are
  available only via `Add Device...` / Local Mesh settings. If a hidden saved
  mesh source needs attention, its protocol chip carries the warning state so
  the operator does not need to scroll or hunt for the issue.
- Local Mesh settings treat devices as a saved-device library. Scans produce
  candidate devices; saving or accepting a candidate adds/updates the library.
  If the live worker is connected to a device different from the selected form
  entry, Settings must show that as connection state, not silently relabel the
  selected configuration.
- Mesh channel review tables must order real named feeds before generated
  placeholders such as `Channel 9`; accepted named feeds appear first, pending
  generated channels appear afterward in numeric order.
- Pending private channels discovered from a connected device must show key
  state as `On device`, not `Joined`, until the user accepts the feed into FIO.
- Check Configuration and BLE pairing guidance must render as normal wrapped
  content in the Local Mesh section, not as a cramped form-row value under
  `Found`.
- Local Mesh settings must hide inactive connection-mode rows. A BLE
  connection should not show blank TCP, USB Serial, HTTP, or MQTT rows. BLE
  scan results are discovery state only; when the saved BLE device is connected,
  a failed/empty scan must not display as the primary connection state.
- policy-gated mesh message promotion: raw packets persist immediately, but
  Inbox/Ops/Map/topic observation visibility requires an accepted channel policy
- accepted channel policies shape rendered mesh observations: mapped groups
  override raw channel labels for group context, and the observation provenance
  retains channel name, privacy, key readiness, selected surfaces, and routing
  context so Inbox, Ops Center, and Map can present the same message
  consistently
- Inbox row projection for accepted MeshCore/Meshtastic observations, including
  source-specific labels, channel/group context, topics, operator-attention
  state, routing/hop detail, and a generic observation detail view
- Mesh Inbox projection must only include operator traffic. Mesh node,
  channel, and topology observations must remain available to Ops Center/Map
  where appropriate, but must not appear as messages. Inbox rebuilds and
  periodic refreshes must reload current mesh observations before applying
  filters so accepted mesh messages remain visible for every age setting,
  including `Any Time`.
- Inbox and detail views must display the accepted channel name for channel
  traffic. Raw placeholders such as `channel`, `CHANNEL`, or a protocol channel
  id must not appear in the operator-facing `To` column when the policy knows a
  real channel name.
- Ops Center topic corrections must be durable for mesh messages. Operator
  corrections are keyed by mesh message `source_ref` and must survive future
  device/channel reprojection. This is required for false-positive topics such
  as casual/router-related `flood` wording without weakening true flood/weather
  detection for real operator reports.
- Map must expose a Mesh traffic selector. MeshCore/Meshtastic observations are
  mappable when GPS/lat-lon, grid, or a route-derived located relay/router is
  available; otherwise they remain Inbox/Ops traffic until location evidence
  arrives.
- Map must also expose a Mesh Nodes selector for topology observations. Mesh
  Nodes consumes `mesh-node:` projections only and must auto-fit once when the
  operator enters the view. Subsequent live updates must keep the existing
  viewport stable.
- per-channel retention pruning for raw mesh messages and their observation
  projections during runtime maintenance
- Inbox `Mesh` focus routing for MeshCore and Meshtastic source families
- `SourceConnectionSnapshot` lifecycle projection for retained MeshCore health
  rows. APRS and future local sources must implement the same contract before
  they appear in Ops Center or Map.
- Map projection worker boundary for retained markers, observations, nodes, and
  routes. Live connection state is intentionally outside the projection
  signature so reconnect churn does not redraw the map.
- conservative worker reconnect retry for enabled adapters that become
  disconnected
- Tests for config validation, lazy package loading, packet normalization, manager lifecycle, persistence, projection, and view-contract coverage

Not implemented yet:

- reconnect/backoff UI and persisted retry countdown diagnostics
- explicit disconnect/reconnect actions in Station Control Center
- editable channel/group mapping controls
- secure private-channel join workflow for entering/importing encryption keys,
  writing them to supported mesh devices, and never exposing raw keys in normal
  configuration or logs
- channel-aware Inbox subfilters
- map-layer rendering of projected mesh node observations
- MQTT bridge
- Reticulum bridge policy

## Next Slice

The runtime foundation is now present: `MeshConnectionWorker` is started from
`MainWindow` only when Local Mesh is enabled, moved to a `QThread`, restarted
when mesh settings change, and stopped during shutdown. It publishes normalized
passive messages, node snapshots, and health into the mesh store and observation
pipeline. Live send remains disabled until the user explicitly enables it per
adapter.

After passive live receive is exercised against real hardware, add:

- connection-state UI for MeshCore BLE: connected, disconnected, retrying,
  pairing needed, not found
- manual disconnect/reconnect controls and conservative auto-reconnect backoff
- real-hardware MeshCore passive receive QA for channel discovery, public and
  private feed review, direct messages, and route/hop hints
- mesh topic/tag normalization and severity policy for Inbox/Ops/Map
- map-layer rendering of mesh nodes with staleness and confidence labels
- source-aware Inbox filters for mesh messages
- per-adapter Settings tabs or chips for multiple local mesh devices
- test connection button that runs in the background
- guided MeshCore BLE pairing/test-connection workflow in Settings
- MeshCore Companion command queue and send support only after receive identity
  and channel behavior are validated
