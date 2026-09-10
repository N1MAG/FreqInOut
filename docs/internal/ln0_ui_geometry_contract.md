# LN-0 UI Geometry And Seam Contract

Status: audit-only contract.  No production UI or configuration data was changed.

Date: 2026-09-09

## Scope and governing decisions

This contract implements the LN-0 wireframe requirement from
`local_nets_tools_resources_implementation_plan.md`.  It is subordinate to the
Where/When/What/Why split: the Station Control Bar remains the radio-specific
Where/When surface; Resources and Local Nets own What/Why.  Local Nets is
awareness only.  It must never visually or behaviorally inherit a QSY action.

### Audit evidence

| Surface | Existing seam | LN-0 conclusion |
| --- | --- | --- |
| Lazy navigation | `main_window.py:351-370`, `372-394`, `5581-5608` | Add stable labels `Resources` and `Local Nets`, placeholders, and factories before adding buttons. Do not construct either at startup. |
| Full navigation | `main_window.py:420-443`, `11143-11164` | Change the present `Plan Builder` master to `Plans`; add Local Nets there. Add Resources as its own master group, not Station/Settings. |
| Compact navigation | `main_window.py:3543-3558`, `3573-3600` | Add a Resources catalog icon using the shared navigation canvas. Route it to a Resources flyout, never directly to Frequency Catalog. Change Plans target from the arbitrary `Plan Builder` screen to the `Plans` group. |
| Shell response | `main_window.py:3307-3341`, `4869-5016` | Keep shell behavior untouched for LN-2/LN-4, but new tabs must tolerate its width changes and vertical compact command bar. |
| Current resource UI | `net_schedule_tab.py:426-517`, `594-668` | The embedded 18-column Net Row Library is legacy compatibility only. It cannot become the Resources UI or remain a second primary editor after cutover. |
| Schedule Outlook | `controlfreq_tab.py:1233-1274`, `7051-7166`, `7173-7242` | Reuse its timeline/card idiom and refresh gate, but use a separate Local Nets subsection/projection rather than adding Local rows to commandable HF rows. |
| Current SOP handoff | `controlfreq_tab.py:7392-7399`, `7457-7466` | Existing navigation only selects a tab; it carries no identity, draft, or return state. LN-5 needs a typed handoff payload. |
| Existing Local-Net-like data | `sop_tab.py:293-305`, `744-797`, `1224-1238`; `traffic_actionability.py:479` | `local_net_profiles` already exists as Settings-shaped, display-label data. It is not the proposed Local Nets model. Migration/compatibility disposition is a gate decision. |
| Shared presentation | `theme.py:19-74`, `main_window.py:3530-3541`, `controlfreq_tab.py:1402-1445` | Use theme tokens, `control_height_for_font`, table internal scrolling, and explicit wide/compact reflow. No fixed-height text controls. |

## Shared geometry tokens

Use these values as target geometry, not hard fixed widget heights.  `u` is the
current body-font line height; apply `control_height_for_font` for text controls.

- Workspace outer margin: 12 px Normal / 14 px Large; inter-region gap: 8 px
  Normal / 10 px Large.
- Header: title + Help + primary action. Header actions wrap below the title at
  less than 760 px usable workspace width; never overlap the shell.
- Standard control row: `max(36 px, u + 16 px)` Normal and `max(44 px, u + 18
  px)` Large. Icon-only targets are at least the same square and have labels or
  accessible names/tooltips.
- Detail surface: 360–460 px in wide mode; result surface is elastic with a 520
  px minimum. At compact widths, result surface is above detail; detail is a
  collapsible inline panel or a modal/drawer when editing.
- Result rows: 48 px Normal / 60 px Large minimum. Rows/cards may grow for a
  wrapped name, but action placement is stable.
- Chipped filter rows wrap before controls shrink below 120 px. A compact,
  internally-scrollable chip strip is allowed; page-level horizontal scroll is
  not.
- All bounded table/list panes own their vertical scrollbars. Headers, primary
  action, selection state, validation, and save/cancel remain outside a long
  result list.

### Screen budget

The dimensions below are *workspace width after rail*, and include the currently
visible Station Control Bar above it. Treat a row in the following table as an
acceptance geometry fixture.

| Window / text | Expected shell | Tab geometry mode | Required protected surface |
| --- | --- | --- | --- |
| 1920x1080 Normal | full rail, command bar wide | wide split | 600 px or more vertical primary list/table after header/filter |
| 1000x700 Normal | compact rail likely; command bar can be multi-row | compact stack | 260 px primary list after summary/header; detail collapsed until selected |
| 900x560 Normal | compact rail; shell may take several rows | short compact | summary + one primary action + at least two result rows; editor opens as a full-height workflow page/drawer |
| 1920x1080 Large | full or compact rail based on measured text | wide split with 420–500 px detail | at least five readable result rows; secondary buttons may overflow |
| 1000x700 Large | compact rail | compact stack | primary action and selection retained; chips use overflow; detail is collapsed/drawer |
| 900x560 Large | compact rail | one-column short compact | no side-by-side fields, no permanent detail; workflow uses one step per page with sticky Back/Continue |

`main_window.py:905-907` currently forces a 900x600 minimum. That blocks a
literal 900x560 acceptance run and must be resolved before the LN-0 exit gate;
the solution may be a revised minimum height or a testable content viewport, but
must not simply waive the specified viewport.

## Wireframes

### Resources — Frequency Catalog

```
WIDE (>= 1200 workspace px)
┌ Tools & Resources · Frequency Catalog ─ Help ─────────────── + New resource ┐
│ Search [label, frequency, group, locality…________________]  [Import/Export] │
│ [Amateur] [GMRS]  [Reference] [Station] [Imported]  [Active] [Updates] [Retired]│
├───────────────────────────────────┬───────────────────────────────────────────┤
│ Results  1–200 of N               │ Selected resource                         │
│ Label / service / where / source   │ name · kind · service · health badges     │
│ ───────────────────────────────── │ Receive / transmit / tone / mode           │
│ North repeater · GMRS · …          │ Reference guidance / provenance / version  │
│ Regional 2 m range · ref · …       │ Used by: 3 schedules  [View impact]        │
│ … internal scroll …                │ [Edit] [Clone to Station] [Retire …]       │
└───────────────────────────────────┴───────────────────────────────────────────┘
```

Result row required content is a friendly label, service, concise Where,
source/status icon plus text, and `Used by` count if nonzero. The detail pane
contains the long reference citation, coverage, grid, notes, versions, and
destructive impact. A range has a disabled `Use for schedule` explanation and
offers `Create operational frequency` instead.

At compact/short mode, title/action, search, and filters remain above a full-width
result list. Selection replaces the list with Detail only on 900x560 Large; a
Back control restores exact query, chips, selection, and scroll offset. `+ New`
opens the same editor and returns its stable resource key to the invoking picker.

### Resources — Net Directory

```
┌ Tools & Resources · Net Directory ─ Help ─────────────── + New Net ┐
│ Search [net, group, place, service…________________] [My groups] [Active] │
├───────────────────────────────────┬───────────────────────────────────────────┤
│ Directory results                 │ Net identity / selected published session  │
│ Net name · group · service         │ purpose, scope, source, verification       │
│ • Wed 19:00 Local · 146.xxx …      │ Sessions (bounded internal list)            │
│ • Sun 18:00 Local · channel …      │ [Add Session] [Add to HF Nets]              │
│ source · Used by N                 │ [Add to Local Nets] [Edit] [Retire…]        │
└───────────────────────────────────┴───────────────────────────────────────────┘
```

`Add to HF Nets` and `Add to Local Nets` remain disabled until one compatible
published session is selected, with Why text. A previously subscribed session
uses `Scheduled` + `Open Schedule`; never render a second ambiguous Add.
Compact mode is List -> Detail -> flow, preserving the list snapshot on Back.

### Add to HF Nets (one shared flow from HF Nets or Directory)

```
Step 1 Select sessions → Step 2 Destination → Step 3 Review
┌ Add to HF Nets                                      [Save] [Cancel] ┐
│ Source: Directory / Create New Net      Draft: 2 selected sessions   │
│ [Search / My Groups / Known Nets / Custom]                            │
│ Session list (checkboxes; selected stays visible)                     │
│ Destination: [Named HF Net schedule v]  Target/radio: [derived…]      │
│ Review: recurrence · time · freq · early check-in · SOP conflict      │
│ Why: “Saving uses existing validation, RF Guard, reprojection…”       │
└──────────────────────────────────────────────────────────────────────┘
```

At 1920 wide, Steps 1 and 2 may share a two-column page and Review is a right
drawer. At 1000 and every Large mode, use a single-column stepper. At 900x560,
make `Back`, `Continue`, and `Cancel` sticky; selection count is a text badge.
Save only exists at review. Conflict/refusal explanations appear directly above
the sticky action bar and do not displace it.

### Plans — Local Nets default and editor

```
┌ Local Nets ─ Help ───────────────────────────────────── [Add Local Net] ┐
│ NOW: —   NEXT: Thu 19:00 MDT (in 38m)   Today 1 · Upcoming 4           │
│ Attention: 1 resource update available                                      │
├ Filters: [All] [Group v] [Service v] [Enabled] [Needs review] ─────────────┤
│ Thu 19:00 MDT · Community / Unassigned · GMRS  |  Net name                  │
│ Channel/frequency · reminder 15m · SOP available  [Edit] [Pause] [Open SOP]│
│ … ordered next-occurrence rows, internal vertical scroll …                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

The default list is not a calendar-first radio scheduler. It must visibly state
`Reminder only — FIO will not tune a radio` in the empty state and final review,
not as a persistent large warning on each row. Active occurrence gains
`Dismiss This Occurrence`; dismiss does not replace Pause.

Editor conceptual pages: `Net`, `Session`, `Group`, `Where`, `When`, `Reminder
& SOP`, `Review`. Wide mode may combine Net+Session and Group+Where; compact
never combines more than one dense selection fieldset. The selected resource is
a chip/card with source, service, Where, and health; `Manage Frequency Catalog`
and `Create custom resource` are contextual handoffs. Final Review includes
exact local and UTC next occurrence(s), selected snapshot/update behavior,
optional SOP with manual wording, and the non-QSY statement.

### Ops Center — Local Nets Outlook subsection

Place immediately after the existing Schedule Outlook timeline, inside the same
Schedule Outlook group but as a visually bounded, collapsible subsection:

```
Schedule Outlook
  [existing commandable HF/SOP timeline]
  ─────────────────────────────────────────────────────
  Local Nets                                         [Hide]
  NOW  Net name · Group · GMRS / channel · ends 19:30  [Details] [Dismiss]
  NEXT Thu 19:00 MDT · in 38m · frequency · SOP ready [Details] [Open SOP]
  Later (4) [Show]
```

Use the existing timeline row spacing, icon treatment, and text+color urgency
language (`controlfreq_tab.py:7173-7242`), but add a non-commandable/local icon
and literal `Reminder` / `Local Net` type text. Actions are exactly Details,
Dismiss when active/reminding, and Open SOP when linked. **No QSY**, no radio
target selector, and no use of `_schedule_qsy_meta`. The section queries active,
next, and at most 50 later entries only; if collapsed, it does no presentation
refresh work beyond a cheap count/urgency invalidation.

## Cross-screen state and handoff contract

Implement one Qt-free, typed `NavigationIntent`/draft snapshot carried by the
main-window routing seam, rather than direct `_navigate_to_tab(label)` calls.

Required fields: `origin_surface`, `return_route`, `return_scroll_y`,
`return_query`, `return_filters`, `return_selection_key`, `draft_id`,
`directory_entry_id`, `directory_session_ids`, `frequency_resource_id`,
`group_id`, `local_net_schedule_id`, `sop_id`, and `readonly_reason` where
applicable. IDs are stable; labels are display-only.

- Picker -> Resources / group editor: serialize unsaved editor draft before
  route; cancel returns exactly to it; save returns the selected stable key and
  restores focus to its field.
- Directory -> HF flow and HF flow -> Directory use the same draft ID and review
  route. HF save returns to the invoking screen’s selected session, not a blank
  HF Nets table.
- Local Nets -> SOP opens a *contextual SOP preview* with the Local Net schedule
  and occurrence IDs. Back returns to the same occurrence. It must not activate
  an SOP.
- Ops -> Details opens Local Nets detail read-only first; Open SOP carries the
  context payload; Dismiss returns to the same Outlook subsection after refresh.

Current direct label navigation (`controlfreq_tab.py:7392-7399`, `7457-7466`)
does not meet this contract and must remain only as a legacy fallback.

## Implementation seams and acceptance checks

1. `main_window.py`: register Resources/Local Nets lazy placeholders and factory
   labels, add full master groups and compact flyout routing, and recalculate
   shell layout when rail collapse changes width. Do not import the new widgets
   eagerly.
2. `resources_tab.py`: owns the shell/tabs and injected `NavigationIntent`.
   `frequency_catalog_view.py`, `net_directory_view.py`, and `resource_picker.py`
   own no navigation hierarchy or persistence schema.
3. `net_schedule_tab.py`: after canonical parity, replace library mutations with
   the contextual picker/deep-link. The legacy library may be retained only as a
   clearly deprecated compatibility view, never as the second editor.
4. `local_nets_tab.py`: own list/editor UI only; it receives immutable bounded
   models and routes resource/group/SOP intents through the host.
5. `controlfreq_tab.py`: add a bounded Local Nets projection adapter and
   subsection renderer; do not modify scheduler actions or HF schedule row
   interpretation.
6. `sop_tab.py` / `sop_manager.py`: add only stable Local Net/session context
   references. Do not promote existing `local_net_profiles` display data into a
   canonical relationship.

Manual visual gates: Light/Dark; Normal/Large; one radio, two radios + Mesh,
and three-radio stress whenever nav/shell changes; no clipped focus ring; no
page-level horizontal scrolling; primary list remains reachable; status updates
do not move sticky actions.

## Gate findings and resolved decisions

1. **Viewport:** current app minimum height is 600, while the target is 900x560
   (`main_window.py:905-907`). LN-2 will reduce the minimum during its shell
   change; the wireframe gate uses the target geometry and does not waive it.
2. **Navigation authority mismatch:** the full master is named `Plan Builder`,
   and compact Plans routes to that child (`main_window.py:518`, `3543-3558`),
   contrary to the product contract’s group hierarchy. The locked decision is
   to rename the master `Plans`, route compact Plans to its flyout, and add an
   owned Resources catalog/library icon in LN-2.
3. **Legacy local profile disposition:** Settings-backed `local_net_profiles`
   already feeds SOP behavior (`sop_tab.py:744-797`). LN-1/LN-4 must specify
   preservation and compatibility. The locked decision preserves them, allows
   reviewed parseable resource seeding, and never treats them as schedules.
4. **Handoff infrastructure missing:** existing cross-tab routes discard
   source/draft/selection (`controlfreq_tab.py:7457-7466`). LN-2, LN-3, LN-4,
   and LN-5 implement the locked typed navigation-intent seam.
5. **SOP payload:** current SOP local actions are
   configuration rows keyed by labels and action types (`sop_tab.py:1224-1238`).
   The locked payload uses `local_net_schedule_key`, optional `net_session_key`,
   and runtime-only occurrence context; schedule linkage uses the existing
   `sop_profile_id` identity and does not activate the SOP.

These decisions are authoritative in
`local_nets_resources_ln0_architecture_decisions.md`.
