# FLDigi / SSB NCS Workbench Spec

## Purpose

FLDigi / SSB NCS is a live net cockpit. It must help an operator run a fast,
dynamic net with minimal distraction, clear radio/session ownership, and quick
role-aware actions for NCS and ANCS duties.

This screen is not primarily a configuration page. Setup remains available, but
the roster, traffic, role ownership, and post-net outputs own the user's
attention.

## Mental Model

The operator should understand the screen in this order:

1. Which radio/session is this net running on?
2. Is the net idle, running, or complete?
3. Who is acting as NCS and ANCS right now?
4. Which stations have checked in, need ACK, have traffic, or need relay?
5. What summary or handoff can be copied when the net completes?

## View Contract

FLDigi NCS publishes an `NcsSessionSnapshot` for the shell, Ops Center, and any
future NCS summary surfaces. The snapshot must include:

- protocol: `FLDigi/SSB`
- radio/source id and short name
- role for the visible operator workspace
- net name when known
- timing state: `idle`, `started`, `active`, or `ended`
- started/ended UTC timestamps when known
- accepted check-in count
- traffic count
- relay or ACK-pending count when known
- NCS callsign and ANCS callsign when assigned

Ops Center consumes this snapshot summary only. It must not inspect widgets from
the FLDigi tab or trigger FLDigi file/log parsing on the UI thread.

## UI Layout Contract

### Session Bar

The top of the tab contains a compact session bar:

- radio short-name chips for eligible FLDigi-capable radios
- net status chip: `Idle`, `Running`, or `Complete`
- `Start Net` and `End Net` on the same row as the radio chips when space allows
- net name selector/edit field
- concise current-session label using radio short name, role, and net name

QSY controls are not duplicated here. Radio movement belongs to the Station
Command Bar. The NCS screen may show read-only current radio/band/frequency
context, but live QSY/Hold actions route through the command bar.

### Role And Assignment

Role and alternate-role assignment are a single compact section.

Required controls:

- role selector: `NCS`, `ANCS`, `Joiner` for current compatibility
- NCS callsign assignment
- ANCS callsign assignment
- quick reassignment when an ANCS station changes mid-net

Labels and action scopes must use callsigns when known. The correct wording is:

- `Action for: NCS <callsign> | ANCS <callsign>`

Do not use `Send to` language here. These actions direct or scope net-control
work; they are not messages sent to the NCS or ANCS station.

If a callsign is unknown, fall back to `NCS` or `ANCS`, but update immediately
when the station is assigned or edited.

### Optional Tools

Macro setup is an optional operational tool. Log-assisted intake remains hidden
until the parser is trusted enough for live NCS use.

- Macros should be collapsed by default after they are configured.
- The visible macro prompt must be a compact disclosure control, not a full-width
  banner or framed strip. It should sit with session/help tools, fit its text,
  and leave the main width for roster work.
- The visible summary should be short: `Macro: Ready`, `Macro: Needs setup`, or
  `Macro: None`.
- Log-assisted capture must not be visible in the normal NCS workflow for now.
  When restored, it must be clearly marked as assisted/experimental until the
  parser is reliable enough for unattended NCS use.
- Collapsing optional tools must not hide net state, role assignment, roster
  actions, or post-net summary actions.

### Roster Workspace

The check-in/roster table owns the main body of the page.

Primary actions stay near the roster:

- add or import check-ins
- mark TFC, QRU, LATE
- ACK-needed workflow
- next traffic
- copy roster summary
- copy summary by state/province after the net ends
- roster compare reminder/action

The session and setup controls must fit a normal laptop-width main window
without forcing page-level left/right scrolling. Net name, next scheduled net,
operator lookup, macro selection, and macro path fields are bounded controls.
Session status such as `Next` and `Now` must render as a single-line status
summary with full detail in a tooltip, not as a tall wrapped field. Long tables
may keep their own horizontal scrollbars, but the FLDigi NCS page itself scrolls
vertically only.

The roster must preserve role-aware rows. NCS and ANCS rows remain pinned and
can be reassigned without losing accepted check-ins.

Roster actions must use operational language:

- `Managed By:` scopes roster state changes to the local NCS, ANCS,
  shared rows, or all rows. When NCS/ANCS callsigns are known, the chips include
  those callsigns.
- `Actions:` contains actions used during the live net, including
  ACK-needed, next traffic, TFC, QRU, LATE, and all check-ins.
- Roster compare is not a hidden advanced concept. The roster header includes a
  compact status/action chip such as `Compare Rosters`, `Compare: Match`,
  `Compare: NCS +2`, or `Compare: ANCS +1`. Selecting the chip expands `Roster
  Compare`, scrolls the NCS page to that section, and opens `Reference` when
  the next step is setting or pasting a partner roster.
- `Copy Gap` is the one roster-gap action. It copies the current NCS/ANCS delta
  directly to the clipboard. When both directions exist, the output includes
  `Missing from NCS` and `Missing from ANCS` sections.
- `Post-net:` is hidden while the net is active and appears only when there is
  a completed net with accepted check-ins to summarize.
- `End Net` must be one operator decision point. The confirmation may include a
  `Copy state summary when net ends` option when accepted check-ins exist, but
  ending the net must not cascade through save/import/empty-log popups.

### Post-Net Actions

After the net is complete, show a compact post-net action strip:

- save/archive log
- copy roster summary
- copy summary by state/province
- export/open output location when available

`Copy Summary by State` counts accepted/confirmed aggregate check-ins across
NCS and ANCS. It does not split counts by who logged the station. A row is
counted once after roster merge/deduplication. Unknown or blank state/province
values are grouped as `Unknown`.

`Copy Summary by State` is not a live-net action. It must stay hidden or
disabled until the net has ended so NCS operators are not distracted by
post-net sharing tasks while directing traffic.

Example output:

```text
Net check-ins by state/province: CO 8, WY 3, NM 2, Unknown 1. Total: 14.
```

Log-assisted entries are included only after they have been accepted into the
roster. Raw assisted candidates are not counted.

## Responsiveness Contract

- Log scanning, macro discovery, relay comparison, and broad roster imports must
  run bounded or off the UI thread.
- The tab must mount scroll content during `_build_ui()` and must not remount
  scroll content on state refresh.
- Session refreshes update labels, chips, and snapshots only.
- Reduced-height and Large-text layouts may scroll vertically, but the operator
  must not see large blank gaps before live controls.

## Acceptance Gates

- Initial navigation to `NCS > FLDigi / SSB` shows session controls and roster
  actions without requiring a tab switch or resize.
- Full-screen laptop width does not require page-level horizontal scrolling.
  Wide detail belongs inside table/detail widgets, not the outer NCS page.
- Radio/session ownership is visible before any NCS action can be used.
- Start/end net actions are visible without scrolling on normal laptop height.
- NCS/ANCS action scope shows assigned callsigns when known.
- QSY actions are not duplicated from the Station Command Bar.
- Macro setup can be collapsed/dismissed after configuration and the collapsed
  control is near Help, not in the intake path.
- Post-net `Copy Summary by State` uses accepted aggregate check-ins and groups
  blanks as `Unknown`.
- Post-net sharing actions are hidden during an active net.
- Roster compare is discoverable even when collapsed, and gap copy is one
  coherent action: `Copy Gap`.
- The compare status chip scrolls to `Roster Compare` without changing
  main-window focus, geometry, or the selected main tab.
- Ending a net requires no more than one prompt. Follow-up save/import/state
  summary results are shown inline.
- Ops Center reads FLDigi NCS state from `NcsSessionSnapshot`, not widgets.
- Starting an ad hoc net must not remount the page, change the selected main
  tab, or force the main window behind another window. Any platform focus
  behavior seen during ad hoc start should be treated as a defect.
