# SDR Receiver Control Specification

Status: implementation authority; SDR-0 compatibility/manual UX, SDR-1
receive-only qualification/setup, and MES-4/MES-5 receiver-lane infrastructure
complete; SDR-2 application adapter implemented with live hardware acceptance
still required; later adapters remain sequential gates

Date: 2026-09-10

Guided configuration authority update (2026-09-16):
`guided_radio_software_configuration_spec.md` supersedes this document's older
observer software-selection, RF Guard/Schedule applicability, launch-composition,
and guided-save rules. The receiver adapter, reversible qualification,
receive-only enforcement, and evidence contracts below remain authoritative.
Implementation notes that describe SDR++ plus JS8Call as the only current
observer stack are current-state evidence, not the target product boundary.

GRS-5 integration update (2026-09-17): observer antenna/front-end Receiver
Guard claims now enter the central pair-scoped scheduler arbitration graph.
Verified automatic receiver retunes are held when a matching active peer owns a
declared shared resource; manual/unverified receivers remain reminder-only.
This automated implementation evidence does not close the live SDR-2 hardware
gate.

## Purpose

This specification defines how FIO identifies, explains, and optionally tunes a
receive-only software-defined radio. The operator-facing goal is hardware-first:

> Which SDR hardware can I use, and can FIO tune it or must I tune it manually?

The answer must remain truthful at three separate layers:

1. the SDR hardware is supported by an installed receiver application;
2. that receiver application exposes a control API that FIO understands; and
3. the exact application/API/hardware/OS combination has passed FIO acceptance.

Failure at layer 2 or 3 does not make the receiver unusable. It remains available
as a manually tuned receiver with clear frequency, mode, bandwidth, and launch or
copy guidance.

This specification extends `shortwave_resources_spec.md`,
`multirig_product_ui_contract.md`, `ui_layout_standards.md`, and the existing
device/runtime safety contracts. It does not authorize implementation by itself;
packages and exit gates are in `sdr_receiver_control_implementation_plan.md`.

## Current Implementation State

FIO now provides an experimental receive-only SDR++ RigCTL adapter. It is not a
direct SDR hardware driver, and no hardware/OS combination is labeled verified
until the live SDR-2 acceptance matrix passes.

- Observer SDR profiles store receive-only identity and endpoint information.
- `SDR Follow` remains advisory. Receiver Guard is separate and enforceable:
  declared shared observer antenna/front-end resources produce a blocking
  scheduler hold for automatic receiver retunes while an active peer conflicts.
- FIO has a RigCtlD protocol client capable of setting and reading frequency, but
  the observer policy deliberately prevents observer devices from entering the
  transceiver control path.
- An SDR host and port indicate a configured application endpoint, not proven
  tuning capability. Only the explicit reversible `Test control` flow can save
  matching verification evidence and enable scheduled receiver tuning.

The transceiver RigCtlD client remains separate. Observer profiles use the
receive-only contract and cannot access PTT or the HF transmit scheduler path.

## Operator Language And Compatibility States

FIO uses these exact concepts:

| UI state | Meaning | Available action |
| --- | --- | --- |
| **FIO tuning ready** | The configured application/API is live, reports the required capability, and its hardware path passed acceptance. | `Tune in FIO` plus manual controls |
| **Connected; verify tuning** | The API responds, but FIO has not completed capability/readback validation for this target. | `Verify` plus manual controls |
| **Manual tuning** | The receiver may be used, but FIO has no live, verified control adapter for it. | `Open receiver`, `Copy frequency`, and tuning guidance |
| **Receiver unavailable** | The configured application or device is not presently reachable. | Recovery guidance and manual frequency details |

Do not use a bare **Supported** or **Unsupported** label. Those words conflate
hardware support by the SDR application with verified control by FIO.

Every selected listing or listening reminder retains a manual path. It shows:

- decimal MHz and kHz;
- mode when known, otherwise `Mode not specified`;
- recommended bandwidth only when the source or an explicit FIO rule supports it;
- `Copy frequency`;
- `Open receiver` when a safe configured launcher exists; and
- concise `Tune this receiver manually` guidance.

Launching an application does not claim that FIO tuned it. A success message is
shown only after API readback confirms the requested receive frequency within the
adapter's documented tolerance.

## Hardware-First Compatibility Catalog

The in-product catalog and Help use one source of truth. Each row records:

- hardware family and, when authoritative, models;
- controlling application;
- FIO adapter/protocol;
- application hardware-support status and source version/date;
- FIO implementation status: `Manual`, `Planned`, `Experimental`, or `Verified`;
- verified FIO application version, OS, architecture, and hardware model;
- capabilities: frequency, mode, bandwidth, start/stop, and readback;
- setup notes and package/driver caveats; and
- last verified date.

Application documentation establishes that an application can use a hardware
family. Only FIO hardware acceptance establishes **FIO tuning ready**. An adapter
must not infer the attached model from a manually entered connection name.

### SDR++ through its Rigctl Server

SDR++ exposes a working basic RigCTL server. Hardware availability depends on the
source modules included and enabled in the user's SDR++ build.

| Application support level | Receiver hardware/source families |
| --- | --- |
| Working in the upstream module matrix | Airspy, Airspy HF+, bladeRF, FobosSDR, HackRF, LimeSDR, PlutoSDR, RFspace, RTL-SDR, SDRplay RSP, RTL-TCP, SDR++ Server, and SpyServer sources |
| Beta in the upstream module matrix | Hermes, Perseus, RFNM, network source, Spectran HTTP, and USRP |
| Not an initial FIO claim | unfinished modules and deprecated Soapy source |

FIO controls the selected SDR++ VFO, not the USB/network device directly. The
adapter uses only the basic frequency/mode operations the SDR++ server reports,
requires frequency readback, and has no PTT/transmit surface. Each command uses a
short-lived bounded TCP connection so FIO does not monopolize SDR++'s single
RigCTL client slot. Because
packaged source modules differ by OS/release, FIO displays the application target
reported or selected during setup and does not promise every listed family on
every installation.

### SDRangel through its REST API

SDRangel exposes a versioned REST API and reports device sets and hardware type.
Its receiver-source documentation includes:

- Airspy and Airspy HF;
- bladeRF v1/v2;
- FUNcube Dongle Pro/Pro+;
- HackRF;
- KiwiSDR/network inputs;
- LimeSDR;
- Perseus;
- PlutoSDR;
- RTL-SDR and RTL-TCP/remote input;
- SDRplay RSP1, RSP1A, RSP2, RSPduo, and RSPdx families;
- SoapySDR-backed devices;
- USRP (upstream specifically notes B210 testing); and
- XTRX as experimental/source-built support.

Binary/package contents vary. For example, upstream notes that some Flatpak
builds omit bladeRF and SDRplay v3 support. FIO therefore discovers the live
device set and plugin schema before offering control. It patches only the exact
device/channel settings returned for the selected receiver, then reads the
device/channel state back. A generic TCP-open check is insufficient.

### SDRconnect through its WebSocket API

SDRconnect is the preferred application bridge for SDRplay receivers. Current
vendor documentation states that SDRconnect supports all RSP models except the
original RSP1. This includes current and retained models such as RSP1A, RSP1B,
RSP2/RSP2pro, RSPduo, RSPdx/RSPdx-R2, and nRSP-ST subject to the installed
release/firmware requirements.

Its WebSocket API exposes writable VFO frequency, hardware center frequency,
demodulator, and filter bandwidth, plus read-only streaming and hardware-control
state. FIO must require `can_control`, use VFO frequency for a normal listening
tune, subscribe to property changes where available, and verify readback. It must
not steal an SDRconnect hardware-control lease from another client.

SDRconnect is available for 64-bit Windows, macOS, and Linux. The vendor currently
tests Linux on Ubuntu LTS; Linux Mint is an operator-tested target for FIO but
must not be described as vendor-supported until the vendor says so.

### Gqrx through its remote-control socket

Gqrx remains a useful secondary RigCTL-compatible bridge on Linux and macOS. Its
documented hardware path includes FUNcube Dongle, RTL-SDR, Airspy, HackRF,
bladeRF, RFSpace, USRP, and devices exposed through SoapySDR. FIO must maintain a
separate Gqrx capability/quirk profile because center-frequency and receiver-VFO
semantics can differ from SDR++.

### KiwiSDR

KiwiSDR provides a documented tuned-URL workflow. Initial FIO support should be
`Open tuned receiver`: construct the documented URL from frequency, mode,
passband, and zoom and open it as an operator-owned browser session. This is not
reported as verified persistent API control because FIO does not own or read back
the browser receiver session.

## Direct Hardware APIs

Direct hardware APIs are not the preferred first implementation. Libraries such
as SoapySDR, UHD, librtlsdr/rtl_tcp, libhackrf, libairspy, and the SDRplay device
API normally make the client own device discovery, configuration, sampling, and
often exclusive access. Using them only to change frequency can conflict with the
application producing the spectrum and audio the operator actually uses.

The product rule is:

- prefer controlling the running receiver application;
- do not load vendor drivers or enumerate all devices on the UI/startup thread;
- do not open a device already owned by another application;
- do not claim useful direct-hardware control until FIO also owns or deliberately
  delegates the receive stream, demodulation/audio path, and shutdown lifecycle;
- if a future direct engine is authorized, isolate discovery and driver loading
  in a cancellable helper process with explicit exclusive ownership.

SoapySDR is valuable as a future device abstraction, but it is not an application
remote-control API. UHD is similarly appropriate for a future USRP receiver
engine, not for lightweight Shortwave tuning. RTL-TCP is an I/Q streaming server
whose control channel is coupled to its client stream; FIO should let SDR++,
Gqrx, or SDRangel consume it instead of opening a competing control connection.

Transceiver-class SDRs capable of transmitting remain governed by FIO's existing
radio, RF Guard, PTT, and scheduler contracts. An observer adapter can never
upgrade a device into a transmit-capable path.

## Receiver Control Contract

The Qt-free receiver-control interface is separate from `RigControlClient` and
contains no PTT method:

- `probe(deadline, cancel_token) -> ReceiverIdentity + capabilities`;
- `list_targets(...) -> bounded receiver/VFO choices` where the API supports it;
- `read_state(target) -> frequency, mode, bandwidth, running, control_owner`;
- `set_receive_frequency(target, hz)`;
- optional `set_receive_mode(target, mode, bandwidth_hz)`;
- `verify_state(target, expected, tolerance)`; and
- `close()`.

An adapter reports capabilities rather than inheriting them from its type.
Frequency-only control is valid and remains useful. Missing mode or bandwidth
support leaves those steps manual and says so explicitly.

## Station Coordinator And Endpoint Lanes

`multi_endpoint_scheduler_concurrency_spec.md` is authoritative for coordinator,
endpoint-lane, timeout, retry, status, lifecycle, performance, and release
behavior. SDR integration adds these receiver-specific constraints:

- an automated receiver uses a receive-only endpoint lane with no PTT surface;
- a manual receiver has reminder/acknowledgement state but no control worker;
- an SDR application/VFO or device-set is included in endpoint identity so two
  profiles cannot create competing control owners;
- a tune success requires adapter readback when the API supports it; and
- reconnect never launches an application or takes hardware ownership without
  an operator-authorized configuration/action.

## Configuration And Daily Use

Configuration begins with the hardware the operator recognizes:

1. Name the receiver and choose its hardware family or `Other / manual`.
2. Choose the application used with that hardware.
3. FIO shows the available bridge for that application and a short setup recipe.
4. `Test control` is the explicit operator action: it probes the API, reads the
   selected VFO, moves it by a small bounded amount, verifies readback, restores
   the original frequency, and verifies restoration on the background endpoint
   lane.
5. Only a passed test produces **FIO tuning ready** for that exact configuration.

### One-session guided receiver setup

The receive-only setup is a guided-workflow redesign governed by
`task_oriented_workspace_design_guideline.md` and
`multirig_product_ui_contract.md`.

- **Primary operator task:** Configure and verify an SDR receiver once, then save
  a usable FIO receiver profile.
- **Starting context:** The operator has selected receive-only SDR hardware and
  has its receiver application, normally SDR++, running with the intended VFO.
- **Completion outcome:** One saved observer profile retains either explicit
  manual-tuning state or matching reversible tune/readback/restore evidence and
  the operator's FIO-tuning opt-in.
- **Normal sequence:** `Radio -> Operating Model -> Software -> Connections ->
  Safety -> Schedule -> Review & Save`. A compatible
  enabled receive-only model is selected before software is selected. The Software
  page uses the same radio-owned application concept as a transceiver while
  allowing only integrations that have a reviewed receive-only contract.
  SDR++ remains the receiver application; optional distinct JS8Call and
  receive-only Fast Light instances may be configured through the unified
  instance lifecycle. Receiver Guard and Receive Schedule remain available,
  but transmit-only software, PTT, and transmit scheduling remain unavailable.
- **Primary actions:** `Test control` is primary while the endpoint is ready for
  qualification. `Save Radio` becomes primary on Review. A passed test visibly
  directs the operator to enable FIO tuning and continue; a failed test leaves
  the profile editable and manual tuning available.
- **Essential state and Why:** Application, adapter, target, endpoint, test in
  progress, failure, verified evidence, and FIO-tuning state remain together on
  the receiver card. A disabled action explains the missing prerequisite in
  visible guidance or its focused tooltip.
- **Secondary and advanced work:** Raw host, port, stable target identity,
  executable/profile paths, and explicit launch-at-startup choices remain
  editable in the guided flow. Transceiver controls and unsafe app choices do
  not appear in the receiver's primary scan path.
- **Workspace archetype:** Guided workflow, because endpoint qualification is a
  staged, safety-sensitive operation followed by review and persistence.
- **Responsive behavior:** One dialog scroll owner preserves the task order.
  Step buttons wrap using font-derived controls; form rows wrap at compact
  widths; evidence and failure copy grow naturally without fixed heights.
- **Shared theme/components:** The workflow uses the shared application theme,
  semantic `button_style` roles, guided forms, font-derived controls, existing
  verification summary, and the normal dialog button box. It adds no local
  palette or fixed text-bearing height.
- **Performance boundary:** Field edits, validation, navigation, paint, resize,
  and evidence rendering are cache-only. Only explicit `Test control` submits
  bounded work to the receiver endpoint lane.

An unsaved receiver draft may run the reversible test without creating a
temporary database row. A bounded opaque qualification request ID correlates
the draft, endpoint-lane result, and open dialog. Late, duplicate, or mismatched
results are ignored. Verification evidence remains dialog-local until the
operator saves through normal profile validation; Cancel leaves no profile or
verification state behind. Saved-profile tests use the same request correlation
in addition to profile identity.

After save, an observer profile exposes one direct **Receiver Setup…** action
and a receiver-specific status chip. It must not report **Apps: Needs Setup**
merely because no conventional FLRig, JS8Call, Fast Light, CommStat, or VarAC
flag is enabled. Opening Receiver Setup returns directly to the connection and
verification card rather than an empty app-connections page.

### Receive-only operating model and JS8Call ownership

Every installation contains exactly one protected built-in **Receive-only SDR**
Operating Model before Guided Add Radio needs it. A fresh blank station ensures
that model and the protected transceiver default without creating a radio,
assignment, or runtime-primary projection. It enables Messages, Map,
background ingest, Launch Control, and verified receive-schedule ownership while
disabling the transmit scheduler, Net Control, profile swaps, transmit QSY, PTT,
Compose sending, Expect replies, and every other transmit path. Observer
restore/default-assignment logic resolves to this
model; it never falls back to the transceiver default.

An SDR may own a distinct JS8Call instance for receive traffic. The instance has
its own executable, profile/data paths, API endpoint, manifest resource claims,
and launch-at-startup choice. It is created through the same reviewed software
instance/adoption contract used by other radios, with `replace_existing=False`
for a new assignment. It must never reuse or silently replace another radio's
instance. SDR++ application launch and JS8Call instance launch are separate
reviewed selections in one guided setup.

Built-in **FIO Spotter** is independently selectable for an observer. In that
context it enables receive/decode, forms, watches, and Inbox projection only.
The assigned receive-only Operating Model remains the authoritative boundary
that removes Compose, Expect replies, retrieval transmissions, QSY, and PTT.
The external JS8Spotter application remains unavailable unless it gains a
separate reviewed receive-only contract.

`Configure Automatically` is an explicit discovery action, never a dialog-open
side effect. App detection and JS8Call profile parsing run on one bounded worker
outside Qt's GUI thread. The dialog remains navigable, visibly reports the
search in progress, preserves every operator-entered value, and applies results
only while the initiating dialog remains current. Cancel requests cancellation;
late results cannot create or save a radio.

The guided workflow saves the observer inactive and non-primary, assigns the
selected receive-only model, creates/adopts the optional JS8Call instance, and
only then may activate the receiver. A first/only observer never becomes the
legacy compatibility primary. Cancel writes no radio, assignment, software
instance, manifest, or launch recipe. A recoverable post-radio failure leaves
the observer inactive and reports the precise Settings route for completion;
it does not alter the primary radio's JS8Call configuration.

Observer-owned JS8Call data retains `radio_id` and `js8_instance_id` provenance
for Inbox, Map, and health. The observer is excluded both from all transmit
selectors and at the final send/preflight boundary, including Compose, Expect,
Net Control, and message-retrieval requests. UI hiding alone is not authority.

### Receive-only software launch

Receiver application launch is opt-in and radio-scoped. The first supported
application is **SDR++**. Selecting SDR++ in Receiver Stack creates a suggested
platform launch target (`sdrpp`, `sdrpp.exe`, or `open -a SDR++`); the operator
may browse to a specific executable or application bundle and may disable FIO
launch while retaining manual startup.

- The receiver-application launch recipe uses the existing per-radio
  launch-bundle store; no conventional `use_*` application flag is required.
  A JS8Call receive instance adds its own radio-scoped launch item through the
  software-instance adoption contract without replacing the SDR++ item.
- A checked launch-at-startup choice is persisted during guided setup and is
  honored on the next FIO start once the receiver is active (`Use Radio`). An
  inactive observer retains the recipe but is not launched in the background.
- Every observer launch item carries `execution_scope=receive_only`. The launch
  planner rejects ordinary or transmit-capable entries for observer profiles,
  including entries injected outside guided setup. SDR++ is the approved
  receiver application; JS8Call and Fast Light require their separately
  reviewed receive-only companion contracts.
- A receiver launch item has no PTT, transmit, transmit-scheduler, or transceiver
  radio-control authority. Selecting it does not enable FIO tuning; reversible
  tune/readback/restore qualification remains a separate explicit action.
- Test control does not launch SDR++ and does not require FIO launch ownership.
  It can test an application the operator started independently. At runtime,
  an already-running SDR++ process satisfies cached process readiness and is
  not launched a second time.
- The dialog holds launch intent only in memory. Cancel writes nothing. On Save
  Radio, FIO first obtains the real observer profile ID and then creates or
  replaces that profile's launch bundle. A bundle-save failure reports the
  recoverable partial outcome: the receiver profile was saved, but Receiver
  Setup must be reopened to retry the launch choice.
- The launch target remains editable because packaging varies by operating
  system. FIO creates the launch recipe; it does not rewrite SDR++'s native
  device or module configuration. The operator still selects the RTL-SDR and
  enables SDR++'s RigCTL Server as described below.
- All controls use shared theme roles, font-derived heights, and responsive
  form wrapping. Selection and paint remain cache-only; no process scan,
  endpoint call, or filesystem crawl occurs during rendering.

For SDR++, the operator first selects the intended VFO inside SDR++ and enables
the RigCTL Server module's tuning control. FIO uses the saved `selected-vfo`
label only as stable endpoint identity; SDR++ RigCTL does not expose a named VFO
list for FIO to enumerate. Changing adapter, host, port, or target invalidates
the saved evidence and returns the profile to manual tuning until it is tested
again. Persistence, runtime construction, readiness presentation, and scheduler
binding all validate that exact evidence independently; the persisted state flag
alone never authorizes receiver control.

The hardware and application names remain separate. A label such as `RSPdx in
SDRconnect` is shown to the operator; opaque device IDs, API indexes, and ports
remain in technical details.

From Shortwave or a listening reminder:

- the receiver chooser places **FIO tuning ready** receivers first;
- manually tuned receivers remain visible and selectable;
- `Tune in FIO` appears only for a live verified adapter;
- `Tune manually` is always available and never disabled because an API is
  absent; and
- a failed API tune falls back to the manual card without marking the hardware
  unsupported.

## Performance And Reliability Budgets

- Opening Resources or Shortwave performs no receiver discovery.
- Opening receiver setup paints before probing and acknowledges within 150 ms on
  the Linux baseline.
- Opening or navigating Guided Add Radio performs no application/profile
  filesystem crawl. An explicit Configure Automatically search may take longer
  on a large JS8Call profile inventory, but it never blocks paint, navigation,
  Cancel, or the operating-system window watchdog.
- One explicit probe is bounded to 2 seconds for a local endpoint unless the
  adapter documents a shorter limit.
- API state uses push events where stable; otherwise visible-only polling is no
  faster than once per second and stops when hidden.
- Discovery results are bounded; vendor-driver discovery is never in-process.
- Shutdown cancels work, closes sockets, and joins workers before QObject
  destruction. No background receiver worker may outlive its owner.
- A five-endpoint acceptance station (three radios and two SDRs) can transition
  all healthy lanes without waiting for a deliberately stalled peer; healthy
  dispatch acknowledgement remains within 250 ms of the coordinator decision
  and completes within each adapter's own bounded deadline.
- An eight-active-endpoint synthetic stress/soak is required to expose linear
  polling, thread, connection, and UI costs. Passing that stress gate is design
  headroom, not a promise that every eight-device hardware combination is
  supported.

## Acceptance And Documentation Gate

Each verified combination records the exact hardware model, firmware, receiver
application/version, API mode/version, FIO version, OS/architecture, test date,
and evidence for:

- connection and capability discovery;
- tune plus readback;
- mode/bandwidth when claimed;
- application-owned manual tuning before and after FIO use;
- disconnect/reconnect and application restart;
- busy/ownership denial;
- timeout, cancellation, and FIO shutdown; and
- 30-minute idle/interaction CPU and thread stability.

Help presents two tables:

1. **Hardware you can use manually** — all configured receiver hardware, with its
   application and launch/copy workflow.
2. **Hardware FIO can tune** — only combinations that passed the gate, with the
   exact application/API and verified platform.

The broader upstream application hardware lists are labeled **Application
compatibility (not yet FIO-verified)**. This prevents both false negatives for
manual hardware and false positives for automated control.

## Primary Sources Reviewed

The compatibility catalog must retain the source and verification date rather
than copying these lists into unrelated UI code.

- SDR++ upstream [source/module matrix](https://github.com/AlexandreRouma/SDRPlusPlus)
  and [user manual](https://www.sdrpp.org/manual.pdf), including its Rigctl
  Server setup and source-module qualifications.
- SDRangel [receiver source-plugin matrix](https://github.com/f4exb/sdrangel/wiki/Sample-source-plugins-%28Rx-devices%29),
  [server device list](https://github.com/f4exb/sdrangel/wiki/SDRangel-server),
  [package caveats](https://github.com/f4exb/sdrangel/wiki/Quick-start), and
  [REST API](https://github.com/f4exb/sdrangel/wiki/Web-%28http%29-REST-API).
- SDRconnect [current platform and RSP support](https://sdrplay.com/sdrconnect/)
  and [WebSocket property API](https://www.sdrplay.com/docs/SDRconnect_WebSocket_API.pdf).
- Gqrx [hardware and platform statement](https://github.com/gqrx-sdr/gqrx).
- KiwiSDR [documented tuned URL](https://kiwisdr.com/info/).
- Hamlib [rigctld protocol](https://hamlib.sourceforge.net/html/rigctld.1.html)
  for the common receive-only command seam.
- SoapySDR [device abstraction](https://github.com/pothosware/SoapySDR) and UHD
  [USRP API documentation](https://uhd.readthedocs.io/) for the deferred direct
  hardware assessment.
