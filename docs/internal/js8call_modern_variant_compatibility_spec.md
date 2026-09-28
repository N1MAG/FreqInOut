# Modern JS8Call Variant Compatibility Specification

Status: implementation exit gate passed; Linux package and multi-instance
hardware confirmation remains external release qualification

Date: 2026-09-10

Scope: JS8Call application discovery, launch, managed API configuration,
protocol compatibility, receive backpressure, message timestamp semantics,
per-instance settings and application-data namespaces, multi-radio isolation,
and operator-facing constraints for JS8Call 2.2.0, JS8Call-Improved 3.0.3, and
JS8Call Subspace Edition 4.1.0.478

## Purpose And Authority

FIO treats JS8Call as a radio-scoped external service, not as one fixed vendor
binary. This specification defines the compatibility contract for current
JS8Call-family applications without weakening FIO's multi-radio, automated-send,
or responsiveness guarantees.

The implementation was derived from the exact source trees supplied for review:

- `/Users/bill/RadioTools/Programs/js8_22`;
- `/Users/bill/RadioTools/Programs/js8improved303/JS8Call-improved-3.0.3`;
- `/Users/bill/RadioTools/Programs/Subspace-Edition/Subspace-Edition-4.1.0.478`.

Those trees are review evidence, not FIO runtime dependencies. FIO must continue
to run when neither source tree is installed.

This document supplements `multirig_product_ui_contract.md`,
`multi_endpoint_scheduler_concurrency_spec.md`, and the JS8Call/Expect sections
of `production_reliability_and_workflow_remediation_spec.md`. Where variant
behavior differs, this document is the specific authority.

## Operator Outcome

An operator can select an installed JS8Call-family application or executable
for a radio, including a Linux `.deb` installation, and FIO will:

- launch and recognize the selected application without requiring a particular
  executable capitalization;
- enable the native TCP API requests required by modern variants;
- connect to the radio-scoped API endpoint already configured in FIO;
- ingest native events with their correct UTC;
- display every supported speed mode by name;
- keep busy native event streams bounded and responsive; and
- derive each radio's message files from its actual JS8Call application-data
  namespace rather than from `SaveDir`; and
- clearly prevent local configurations that appear isolated but are known to
  share message storage.

FIO does not claim that selecting a different TCP port alone isolates an
application's settings or data files.

This outcome is implemented. Automatic upstream `--rig-name` construction,
authoritative application-data resolution, storage-collision validation,
canonical-root ingestion, bounded reconciliation, and operator-facing storage
state are now part of the runtime contract.

## Reviewed Variant Matrix

| Contract | JS8Call-Improved 3.0.3 | Subspace 4.1.0.478 | FIO behavior |
|---|---|---|---|
| Linux executable | `JS8Call`; upstream user installer uses `~/.local/bin/JS8Call` | Debian package uses `/usr/bin/js8call-subspace` | Discover canonical, case, package, PATH, and operator-selected executable forms |
| TCP / UDP defaults | 2442 / 2242 | 2442 / 2242 | Retain radio-scoped configurable ports |
| TCP framing | newline-delimited compact JSON | newline-delimited compact JSON | Existing native client framing remains authoritative |
| Command authorization | `TCPEnabled` plus `AcceptTCPRequests` | `TCPEnabled` plus `AcceptTCPRequests` | Managed settings always write both flags |
| Correlation | `_ID` request/response correlation | `_ID` request/response correlation | Existing correlated request path |
| API UTC | epoch milliseconds | epoch milliseconds | Normalize milliseconds to epoch seconds and UTC display text |
| Speed values | 0 Normal, 1 Fast, 2 Turbo, 4 Slow, 8 Ultra | same plus 16 Subspace | Shared display helper covers 0, 1, 2, 4, 8, and 16 |
| Selected call getter | `RX.GET_CALL_SELECTED` | `RX.GET_CALL_SELECTED` | Used by automated-send preflight |
| Selected call setter | no setter implemented | `RX.SET_CALL_SELECTED` | Prefer the Subspace spelling, then send legacy aliases as best effort |
| Send acceptance | `TX.SEND_MESSAGE` is fire-and-forget | successful legacy send is fire-and-forget; refusal may be reported asynchronously | A successful local write means submitted to JS8Call, not RF transmission confirmed |
| TX completion | no positive completion event | `TX.COMPLETE` extension | Additive evidence only; core behavior cannot require it |
| Configuration probes | includes `STATION.GET_CONFIG` and `RX.GET_FREE_OFFSETS` | those two are absent | Missing optional probes remain nonblocking capability warnings |
| High-volume events | emits `TX.FRAME` with tone arrays | may emit transmit events and richer extensions | Drop unused `TX.FRAME` at the shared hub and bound all polling backlogs |
| Multi-instance storage | `--rig-name` changes application/settings/data identity | Per the approved forward-compatibility assumption, `--rig-name` changes application/settings/data identity in the corrected Subspace build | Plan each variant with a stable unique rig name, API ports, and application-data root; show `Needs verification` until runtime evidence confirms the root |

## Verified Storage Namespace Behavior

The term **message storage** means the JS8Call writable application-data root
that owns `ALL.TXT`, `DIRECTED.TXT`, `inbox.db3`, and related runtime data. It is
not the configurable `SaveDir` used for saved/received transfer files.

| Variant | Settings and lock identity | Message-storage identity | Required FIO policy |
|---|---|---|---|
| JS8Call 2.2.0 | The application starts as `JS8Call`; `--rig-name <name>` changes it to `JS8Call - <name>` before settings and lock creation | `QStandardPaths::DataLocation` is resolved after the application-name change; every distinct rig name therefore receives a distinct writable data root | Generate and pass a stable unique rig name for every concurrent local instance; resolve messages from that instance's application-data root |
| JS8Call-Improved 3.0.3 | Same application-name and rig-name sequence as 2.2.0 | Uses the newer `QStandardPaths::AppLocalDataLocation`, resolved after the application-name change; message data remains per rig name | Same as 2.2.0 |
| Subspace corrected-build contract | `--rig-name` suffixes its settings file and lock | Per maintainer direction, assume the corrected build resolves application data after applying the rig name, matching 2.2.0 and Improved 3.0.3 | Permit distinct planned instances, retain `Needs verification` until each live root is observed, and fall back to one ingest owner if roots collide |

For 2.2.0 and Improved 3.0.3, distinct TCP ports, MultiSettings configuration
names, or `SaveDir` values do not substitute for distinct `--rig-name` values.
The rig name is the namespace key that Qt uses when resolving the application
data location.

FIO selects these rules from verified variant/version capability evidence, not
from the executable basename alone. An unknown or ambiguous build remains
`unverified` until an observed application version and storage root establish
its behavior. This prevents a renamed binary or downstream package from being
mistakenly granted rig-scoped storage guarantees.

For all variants, FIO must distinguish five paths or identities:

1. application executable;
2. settings file/configuration identity;
3. instance lock identity;
4. writable application-data root containing message logs and inbox; and
5. `SaveDir` containing operator-saved or received files.

No UI label, migration, discovery helper, or managed-profile builder may treat
items 4 and 5 as interchangeable.

## Discovery And Launch Contract

### Operator selection

The Settings control is named **JS8Call Application**. On Linux and Windows it
accepts an executable file; on macOS it accepts the application bundle or
folder. FIO also accepts an install folder and resolves a known executable
inside it for backward compatibility.

Linux discovery includes:

- `js8call` and `JS8Call` in standard system locations and on `PATH`;
- `~/.local/bin/JS8Call` for the Improved 3.0.3 user installer;
- `js8call-improved` and common Improved folders;
- `/usr/bin/js8call-subspace`, `/usr/local/bin/js8call-subspace`, and common
  Subspace folders; and
- the same names under an operator-selected Radio Apps Base Folder.

Configuration discovery includes rig-named files on every supported desktop
platform, including Improved's `JS8Call - <rig>.ini` convention and Subspace's
`JS8Call-<rig>.ini` convention. Enumeration is limited to known JS8Call naming
families in bounded configuration directories; it is not a filesystem-wide
search.

Windows discovery remains case-insensitive and covers the corresponding `.exe`
forms. Process status recognizes stock, Improved, and Subspace process names as
the JS8Call service.

### Arguments and isolation

FIO launches the exact configured command override when present. For JS8Call
2.2.0 and Improved 3.0.3, managed launch planning must also construct a stable,
unique `--rig-name` from the radio's persisted JS8 instance identity. The value
must not depend on list position, display order, or a transient database id.

If an operator supplies a command override:

- an existing `-r` or `--rig-name` value is preserved and validated;
- FIO must not append a second rig-name option;
- two active local profiles may not use the same normalized rig name; and
- the preview must show the exact command and resolved storage namespace before
  launch.

FIO now constructs a stable rig name for managed launches. An explicit command
override remains supported and its single existing rig-name option is preserved
and validated. The UI does not describe MultiSettings names, API ports, or
FIO's managed `SaveDir` folders as proof of message-store isolation.

Subspace supports `-r` / `--rig-name`. Per the approved product compatibility
assumption, FIO treats its settings, process lock, and application-data namespace
as rig-scoped in the same manner as JS8Call 2.2.0 and Improved 3.0.3. FIO may
therefore plan multiple local Subspace instances only when their rig names, API
ports, and resolved application-data roots are distinct. Until runtime evidence
confirms each resolved root, the UI must show `Needs verification`; the product
assumption is not presented as observed evidence.

Live native API events retain the identity of the radio-scoped host/port that
delivered them, so FIO can attribute that live evidence to its configured JS8
instance. File-derived records are attributed only after the resolved
application-data root is unique and verified. If actual Subspace runtime evidence
contradicts the rig-scoped assumption, FIO marks the affected instances
`Needs attention`, coalesces the shared root to one ingest owner, and does not
assign those file records to an arbitrary radio.

## Storage Resolution And Attribution Contract

### Persisted instance record

Each radio-scoped JS8 configuration must expose one coherent instance record
containing at least:

- stable FIO JS8 instance id and operator-facing radio name;
- variant family and observed version, with `unknown` allowed;
- normalized rig name and its provenance (`managed`, `command_override`, or
  `observed`);
- TCP host/port and UDP port;
- settings file and selected MultiSettings configuration, if any;
- resolved writable application-data root;
- `ALL.TXT`, `DIRECTED.TXT`, and `inbox.db3` paths derived from that root;
- separate `SaveDir` value;
- storage mode (`rig_scoped`, `shared`, or `unverified`); and
- verification timestamp and evidence source.

New persistence fields require an additive, idempotent migration. Existing
paths and source files must not be moved, renamed, copied, deleted, or rewritten
by migration.

### Resolver precedence

Storage resolution must be bounded and deterministic. Use this precedence:

1. an operator-confirmed explicit application-data root;
2. a previously verified root whose settings/rig/variant identity still
   matches;
3. the platform-specific Qt application-data namespace derived from the exact
   effective application name (`JS8Call` or `JS8Call - <rig>`);
4. a bounded known-location discovery match supported by an existing settings
   file, expected message files, and unique radio/rig association; and
5. `unverified`, requiring operator review.

`SaveDir` is never an automatic candidate for the application-data root. A
legacy explicit `DIRECTED.TXT` path may be preserved when it exists, but FIO
must identify it as an explicit override and must not infer sibling ownership
from a `SaveDir` setting alone.

Resolution must account for Qt/platform differences without performing a home
directory or filesystem-wide scan. Candidate directories are limited to the
known Qt configuration/data roots for Linux, macOS, Windows, sandboxed package
locations already supported by FIO, and explicitly selected operator paths.
Platform-derived paths are candidates, not proof: after the process becomes
ready, reconciliation must verify the candidate using bounded settings and
message-file evidence before attributing file traffic to the instance.

### Collision and shared-store policy

Before starting file watchers or local processes, canonicalize the resolved data
roots and build a collision map.

- Two local 2.2.0/Improved profiles expected to be rig-scoped but resolving to
  the same root are a blocking configuration error.
- Two profiles using the same normalized rig name are a blocking launch error,
  even when their TCP ports differ.
- Subspace is planned as `rig_scoped` under the approved compatibility
  assumption, with the same unique rig-name, endpoint, and root checks as 2.2.0
  and Improved 3.0.3.
- Runtime evidence remains authoritative. If two Subspace instances resolve to
  one canonical root, launch/ingest status becomes `Needs attention`; that root
  is watched once and its records remain shared/unattributed.
- A deliberately shared root is watched and ingested once, never once per
  radio. Its file-derived records are marked shared/unattributed rather than
  assigned to an arbitrary radio.

### Evidence attribution

Live native API events are attributed to the configured endpoint and retain
`source_radio_id`, `js8_instance_id`, and endpoint-derived source identity.

File-derived evidence is attributed to a radio only when its canonical
application-data root maps to exactly one verified instance. Identical traffic
received by two isolated instances remains two source observations because the
source identities include the instance/data-root namespace. Content
deduplication may relate those observations but must not erase reception
provenance.

When a root is shared or unverified, FIO must retain the evidence without
inventing a radio id. Ops Center, Messages, Map, and message intelligence should
show `Shared JS8 storage` or `JS8 source unverified` where source identity
matters.

### Runtime reconciliation

At launch and after a JS8 application becomes API-ready, FIO performs a bounded
reconciliation of the expected settings, rig name, endpoint, and storage root.
This does not scan message history or block the UI. A mismatch marks the
instance as needing attention and keeps the last verified mapping until the
operator confirms a replacement; it must not silently reassign historical
records to another radio.

## Managed Configuration Contract

Every generated or rehydrated JS8Call `MultiSettings` profile includes:

- `TCPEnabled=true`;
- `AcceptTCPRequests=true`;
- the radio-scoped TCP server/port;
- the radio-scoped UDP port; and
- the existing FLRig/CAT and station identity fields.

For 2.2.0 and Improved 3.0.3, managed setup additionally owns the effective
rig-name launch argument and derives the message paths from the resulting Qt
application-data namespace. `SaveDir` may still be configured for file-transfer
workflow, but generated `DIRECTED.TXT`, `ALL.TXT`, or inbox paths must not be
placed under it unless an observed running variant proves that location.

The additive `AcceptTCPRequests` setting is required because modern variants
can listen on a socket while refusing API commands when request acceptance is
disabled. No destructive settings migration is required.

## Native API Compatibility

FIO's required baseline remains:

- station/version and available capability probes;
- frequency read/write and PTT state;
- transmit text and queue-depth safety checks;
- receive call, band, and directed-activity events;
- inbox read/store where supported; and
- `TX.SEND_MESSAGE` for existing FIO transmit workflows.

Subspace-only APIs such as `TX.SEND_DIRECTED`, `TX.COMPLETE`, history arrays,
and `MODE.GET_SUBMODE_NAME` are optional enrichments. Core workflows must not
depend on them because Improved and established JS8Call versions do not expose
the same set.

An asynchronous Subspace `TX.SEND_MESSAGE` refusal is recorded in native-client
health as `tx_refused:<reason>`. FIO must not synchronously wait for a positive
send response because compatible legacy and Improved implementations do not
send one.

### Selected target safety

FIO sends `RX.SET_CALL_SELECTED` first when it needs to clear or set the UI
selection, followed by historical aliases for compatibility. It then reads
`RX.GET_CALL_SELECTED` during preflight.

Improved 3.0.3 has no selected-call setter. Consequently, an existing Improved
UI selection cannot be cleared through its API. FIO preserves the current safe
behavior: an unattended automatic send is held when a selected target remains.
The operator can deselect it in JS8Call. FIO must not claim the selection was
cleared or silently weaken this safety gate based only on a guessed variant.

A future relaxation requires an explicit, version-proven capability and a
separate transmission-safety review. It is not part of this package.

## Timestamp Contract

Native event `UTC` accepts:

- epoch milliseconds used by both reviewed modern variants;
- epoch seconds; and
- legacy `YYYY-MM-DD HH:MM:SS` UTC text.

The normalized value is epoch seconds plus canonical UTC text. Invalid or absent
native timestamps fall back to receipt time. This helper is limited to native
API events; parsing of existing `ALL.TXT`, `DIRECTED.TXT`, inbox, and other disk
sources keeps its established semantics.

## Backpressure And Performance

The socket reader, polling consumers, and Qt presentation must remain isolated
from a busy endpoint.

1. The native client's optional polling backlog is bounded at 2,048 events.
   Registered listeners still receive each parsed event immediately.
2. On polling-backlog overflow, FIO removes the oldest queued event and retains
   the newest. The socket reader never waits for UI consumption.
3. The shared receive hub discards `TX.FRAME` because no FIO consumer uses tone
   arrays. It retains `RX.DIRECTED`, `RX.ACTIVITY`, and `RIG.PTT`.
4. The receive-hub backlog is bounded at 2,048 events. Overflow preserves the
   recent event window and increments a diagnostic counter.
5. Hub diagnostics expose queued, capacity, overflow-dropped, and
   disposable-dropped counts without scanning the queue.
6. One slow, busy, or malformed JS8 endpoint must not block another radio or
   the scheduler's endpoint workers.
7. Storage resolution uses cached, bounded known-location checks. It does not
   walk the home directory, rescan retained message history, or poll every
   candidate on a UI timer.
8. One watcher/ingest cursor exists per canonical data root. Isolated roots may
   progress independently; a slow or locked inbox database cannot block another
   instance's API client, watcher, or projection work.

## UI Rules

- Use application/executable language, not install-folder-only language.
- Do not show real callsigns in hints or examples.
- Keep radio identity and API endpoint visible wherever the operator could
  confuse one instance with another.
- Do not expose source-code variant implementation detail during normal use.
- A missing optional API probe is a capability limitation, not a false
  disconnected state.
- Subspace's rig-scoped behavior is an explicit compatibility assumption, not a
  verified claim. Settings must distinguish `Needs verification` from observed
  isolation and identify both radios if a root collision is detected.
- Show an operator-readable **Message storage** state for each JS8 radio:
  `Isolated · <rig name>`, `Shared`, or `Needs verification`. Keep full paths in
  details/help rather than crowding the primary radio card.
- `Save folder` and `Message storage` are separate labels and controls. Help
  text must explain that changing the save folder does not relocate message
  logs or the inbox database.
- If FIO detects a duplicate rig name or data root, identify both affected radio
  names and provide one direct action to review their JS8 configuration.

### Hotfix: legacy default-profile launch preservation (2026-09-28)

Status: **Awaiting maintainer pass approval**.

Observable production case: an upgraded single-radio station whose linked JS8
instance still owns `default_js8_instance` and the default JS8 application-data
root must launch the configured package executable with its established native
default settings. FIO must not turn the old migration key into a new
`--rig-name`, because JS8Call uses that value for its settings filename as well
as its visible application name.

The bounded compatibility rule is:

- when the default migrated instance has no rig name, or only the exact old
  FIO-generated fallback for `default_js8_instance`, launch the configured
  executable without `--rig-name`;
- retain its configured API endpoint, save/forms paths, verified message root,
  and native default settings namespace without copying, rewriting, or moving
  any JS8-owned file;
- apply `JS8Call — <radio name>` only as a best-effort PID-scoped desktop title
  after launch, so presentation cannot select a different JS8 profile;
- preserve any explicit operator-selected rig name, including one attached to
  the default database row; and
- keep every non-default managed or adopted JS8 instance on the existing unique
  `--rig-name`, endpoint, and storage-isolation contract.

The supplied production-shaped database is the acceptance fixture: its FTDX-10
record uses `/usr/bin/js8call-subspace`, endpoint `127.0.0.1:2442`, default
message root `/home/bill/.local/share/JS8Call`, and the obsolete generated rig
name `fio-default_js8_instance-50f8a9bb`. The corrected plan must remove only
that generated argument while preserving the other configured facts.

### Hotfix follow-up: surviving generated-profile process (2026-09-28)

Status: **Awaiting maintainer pass approval**.

The production pull/restart log proved an in-place transition case that the
launch-plan fixture did not cover. JS8Call launched before the update with
`--rig-name fio-default_js8_instance-50f8a9bb` remained alive after FIO exited.
The updated no-argument native-default recipe then treated that process as an
exact match and suppressed the intended default-profile launch. FIO must not
terminate an external application or start a competing instance on the same
endpoint to resolve that ambiguity.

After the launch-owned process inventory is fresh, a legacy-default JS8 item now
checks the matching executable's argv for only the obsolete
`fio-default_js8_instance-*` selector. If found, Launch Control fails closed with
an explicit instruction to close that JS8Call window/process and choose Launch
again. A true argument-free native-default process remains eligible for normal
endpoint/readiness handling, and operator-selected or non-default rig names are
unchanged.

Automated acceptance covers the surviving generated selector, the actionable
result, the no-spawn safety boundary, and the native-default non-conflict case.
Linux operator qualification remains required: close the old generated-profile
JS8Call, choose Launch once, confirm the installed default settings appear, and
confirm the title becomes `JS8Call — FTDX-10` where the desktop permits it.

## Acceptance Gate

The automated implementation gate is complete. Linux production qualification
listed after the numbered requirements remains an external release gate because
it requires installed applications, live endpoints, and radio hardware.

The API/application package retains its completed tests. The expanded storage
package passed its exit gate after automated proof showed that:

1. Linux discovery recognizes `~/.local/bin/JS8Call`, packaged Subspace, stock,
   and Improved/case variants, plus both reviewed rig-named settings conventions.
2. Direct executable and compatible folder selections resolve to the intended
   command without a shell.
3. Process readiness recognizes the modern process names.
4. Generated and rehydrated profiles contain `AcceptTCPRequests=true`.
5. Millisecond, second, and legacy-text native UTC values preserve event time
   across directed, Spotter, and dynamic FLAMP parsing.
6. speed values 8 and 16 display as Ultra and Subspace.
7. the preferred selected-call command is attempted without dropping legacy
   aliases.
8. optional polling and receive-hub queues remain bounded during a flood;
   listener delivery remains nonblocking; unused `TX.FRAME` is discarded.
9. Subspace refusal is observable without adding a positive-response wait.
10. planning two local Subspace instances succeeds only with distinct rig names,
    endpoints, and proposed application-data roots; unresolved roots remain
    `Needs verification` rather than being presented as observed isolation.
11. JS8Call 2.2.0 and Improved 3.0.3 managed launches receive stable, distinct
    `--rig-name` values and exact launch preview commands.
12. an explicit command's existing rig name is preserved; duplicate or
    conflicting rig-name options are rejected.
13. default and two rig-named fixture instances resolve to three distinct
    application-data roots, with `ALL.TXT`, `DIRECTED.TXT`, and `inbox.db3`
    beneath the correct root.
14. changing only `SaveDir`, MultiSettings name, or API port does not change the
    resolved message-storage identity or satisfy the isolation gate.
15. two isolated instances receiving identical text retain distinct radio and
    instance provenance through file ingestion and normalized projection.
16. a shared or unverified root is ingested once and is never falsely attributed
    to a radio.
17. canonical/symlink-equivalent root collisions are detected without a global
    filesystem scan.
18. legacy explicit paths are preserved non-destructively and surfaced for
    verification; no source file is moved or rewritten.
19. storage resolution and one locked inbox remain off the Qt thread and do not
    delay another endpoint.
20. existing JS8 send, Expect, ingestion, launch, status, and multi-radio tests
    remain green.

Linux production qualification must exercise a default and at least two
simultaneous uniquely rig-named JS8Call 2.2.0/Improved instances: confirm unique
settings, locks, data roots, APIs, and message attribution before and after
restart. It must also exercise at least two corrected Subspace instances:
select each executable/rig profile, confirm distinct API endpoints and resolved
application-data roots, receive directed events with correct per-instance
attribution, observe the correct speed, and perform a guarded
operator-authorized send. If the observed roots collide, qualification fails;
FIO must retain one ingest owner and present the affected instances as needing
attention rather than guessing radio attribution.

## Storage Implementation Packages

### JSV-S1 — Namespace model and resolver (complete)

- add the persisted instance/storage record through an additive migration;
- implement platform-aware Qt namespace derivation for 2.2.0 and Improved;
- keep `SaveDir` separate; and
- cover default, rig-named, explicit, sandboxed, missing, and legacy paths.

Exit gate: requirements 13, 14, 17, and 18 pass.

### JSV-S2 — Launch and collision enforcement (complete)

- generate stable radio-scoped rig names;
- merge or validate command-override arguments;
- show exact preview/storage consequences; and
- block duplicate rig names, duplicate isolated roots, and unsupported shared
  local instances before process launch.

Exit gate: requirements 10, 11, and 12 pass with no process started on failure.

### JSV-S3 — File ingestion and provenance (complete)

- create one watcher/cursor per canonical data root;
- route file-derived evidence through the verified instance mapping;
- represent shared/unverified evidence without false attribution; and
- preserve distinct observations from different isolated roots.

Exit gate: requirements 15, 16, and 19 pass under burst, lock, restart, and
unchanged-source tests.

### JSV-S4 — UI, migration qualification, and integration (complete)

- add concise Message storage state and actionable collision guidance;
- reconcile existing explicit paths without moving source material;
- update operator help and diagnostics; and
- run clean-partition regression plus Linux multi-instance qualification.

Exit gate: all 20 requirements pass; the work log records automated evidence
and the external Linux qualification boundary.

Automated evidence is recorded in `ui_regression_work_log.md`. External Linux
qualification remains pending and does not weaken the requirement to leave an
unverified or unexpectedly shared storage root unattributed.

## Deferred Opportunities

- explicit variant/capability reporting in Station Health;
- Improved filter, group, heartbeat, and auto-reply control APIs;
- Subspace directed-send and transmit-completion enrichment;
- live Subspace multi-instance storage qualification on each supported desktop.

These additions require their own UX and safety review. They are not blockers
for the baseline support defined here.
